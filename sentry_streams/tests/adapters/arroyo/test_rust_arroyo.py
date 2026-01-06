from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    Map,
    Pipeline,
    streaming_source,
)
from sentry_streams.runner import iterate_edges
from sentry_streams.rust_streams import (
    ArroyoConsumer,
    InitialOffset,
    PyKafkaConsumerConfig,
)
from sentry_streams.rust_streams import Route as RustRoute
from sentry_streams.rust_streams import (
    RuntimeOperator,
)


def test_rust_arroyo_adapter(
    pipeline: Pipeline[bytes],
) -> None:
    bootstrap_servers = ["localhost:9092"]  # Test Kafka servers

    adapter = RustArroyoAdapter.build(
        {
            "steps_config": {
                "myinput": {
                    "bootstrap_servers": bootstrap_servers,
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test_group",
                    "additional_settings": {},
                },
                "kafkasink": {"bootstrap_servers": bootstrap_servers, "additional_settings": {}},
            },
        },
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Most of the logic lives in the Rust code, so it can't be inspected here.
    # The consumer that this adapter uses is a pyo3 wrapper around the Rust consumer,
    # so it also can't be replaced with the in-memory broker or triggered manually.
    assert adapter.get_consumer("myinput") is not None


def test_build_pipeline() -> None:
    # Create a consumer
    kafka_config = PyKafkaConsumerConfig(
        bootstrap_servers=["localhost:9092"],
        group_id="test-group",
        auto_offset_reset=InitialOffset.earliest,
        strict_offset_reset=False,
        max_poll_interval_ms=60000,
        override_params={},
    )

    consumer = ArroyoConsumer(
        source="test_source",
        kafka_config=kafka_config,
        topic="test_topic",
        schema="test_schema",
        metric_config=None,
    )

    route = RustRoute("test_source", [])
    consumer.add_step(RuntimeOperator.Map(route, lambda x: x.payload))
    consumer.add_step(RuntimeOperator.Filter(route, lambda x: x.payload))

    assert len(consumer.steps) == 2


def test_get_steps(
    pipeline: Pipeline[bytes],
) -> None:
    """Test that get_steps returns the correct structure of steps per source."""
    bootstrap_servers = ["localhost:9092"]

    adapter = RustArroyoAdapter.build(
        {
            "steps_config": {
                "myinput": {
                    "bootstrap_servers": bootstrap_servers,
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test_group",
                    "additional_settings": {},
                },
                "kafkasink": {"bootstrap_servers": bootstrap_servers, "additional_settings": {}},
            },
        },
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Get the steps
    steps = adapter.get_steps()

    # Verify the structure
    assert isinstance(steps, dict), "get_steps should return a dictionary"
    assert "myinput" in steps, "The source 'myinput' should be in the steps dict"
    assert isinstance(steps["myinput"], list), "Each source should map to a list of steps"
    assert len(steps["myinput"]) > 0, "There should be at least one step in the pipeline"
    assert all(
        isinstance(step, RuntimeOperator) for step in steps["myinput"]
    ), "All steps should be RuntimeOperator instances"


def process_function(msg: Message[bytes]) -> bytes:
    return msg.payload


def test_map_steps_without_sink() -> None:
    """Test that Map steps without a terminal operation are not finalized in get_steps."""
    bootstrap_servers = ["localhost:9092"]

    # Create a pipeline with source -> map -> map (no sink)
    pipeline = (
        streaming_source("test_source", stream_name="test_topic")
        .apply(Map("map1", function=process_function))
        .apply(Map("map2", function=process_function))
    )

    adapter = RustArroyoAdapter.build(
        {
            "steps_config": {
                "test_source": {
                    "bootstrap_servers": bootstrap_servers,
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test_group",
                    "additional_settings": {},
                },
            },
        },
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Get the steps
    steps = adapter.get_steps()

    # Verify that the source exists but has no steps
    # Map steps are only finalized when the chain is closed (e.g., by a sink or other terminal operation)
    assert isinstance(steps, dict), "get_steps should return a dictionary"
    assert "test_source" in steps, "The source 'test_source' should be in the steps dict"
    assert isinstance(steps["test_source"], list), "Each source should map to a list of steps"
    assert (
        len(steps["test_source"]) == 0
    ), "Map steps should not be present in get_steps when no terminal operation closes the chain"
