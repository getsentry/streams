from typing import Mapping, Sequence

import pytest

from sentry_streams.adapters.arroyo.rust_arroyo import (
    RustArroyoAdapter,
    build_kafka_consumer_config,
    build_kafka_producer_config,
)
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.config_types import StepConfig
from sentry_streams.pipeline.pipeline import Pipeline
from sentry_streams.runner import iterate_edges
from sentry_streams.rust_streams import InitialOffset


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
                    "override_params": {},
                },
                "kafkasink": {"bootstrap_servers": bootstrap_servers, "override_params": {}},
            },
        },
        {"type": "dummy"},
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Most of the logic lives in the Rust code, so it can't be inspected here.
    # The consumer that this adapter uses is a pyo3 wrapper around the Rust consumer,
    # so it also can't be replaced with the in-memory broker or triggered manually.
    assert adapter.get_consumer("myinput") is not None


@pytest.mark.parametrize(
    "source_config, consumer_group_override, expected_bootstrap_servers, expected_group_id, expected_auto_offset_reset, expected_strict_offset_reset, expected_override_params",
    [
        pytest.param(
            {
                "bootstrap_servers": ["localhost:9092"],
                "auto_offset_reset": "earliest",
            },
            None,
            ["localhost:9092"],
            "pipeline-test_source",
            InitialOffset.earliest,
            False,
            {},
            id="no_override_params_default_group",
        ),
        pytest.param(
            {
                "bootstrap_servers": ["localhost:9092"],
                "auto_offset_reset": "latest",
                "consumer_group": "my-group",
                "strict_offset_reset": True,
                "override_params": {"session.timeout.ms": "10000"},
            },
            None,
            ["localhost:9092"],
            "my-group",
            InitialOffset.latest,
            True,
            {"session.timeout.ms": "10000"},
            id="override_params_pass_through",
        ),
        pytest.param(
            {
                "bootstrap_servers": ["broker1:9092", "broker2:9092"],
                "auto_offset_reset": "earliest",
                "consumer_group": "config-group",
                "override_params": {
                    "session.timeout.ms": "30000",
                    "heartbeat.interval.ms": "10000",
                },
            },
            "override-group",
            ["broker1:9092", "broker2:9092"],
            "override-group",
            InitialOffset.earliest,
            False,
            {"session.timeout.ms": "30000", "heartbeat.interval.ms": "10000"},
            id="multiple_override_params_pass_through",
        ),
    ],
)
def test_build_kafka_consumer_config(
    source_config: StepConfig,
    consumer_group_override: str | None,
    expected_bootstrap_servers: Sequence[str],
    expected_group_id: str,
    expected_auto_offset_reset: InitialOffset,
    expected_strict_offset_reset: bool,
    expected_override_params: Mapping[str, str],
) -> None:
    """Test build_kafka_consumer_config correctly builds PyKafkaConsumerConfig."""
    result = build_kafka_consumer_config(
        source="test_source",
        source_config=source_config,
        consumer_group_override=consumer_group_override,
    )

    assert result is not None
    assert list(result.bootstrap_servers) == expected_bootstrap_servers
    assert result.group_id == expected_group_id
    assert result.auto_offset_reset == expected_auto_offset_reset
    assert result.strict_offset_reset == expected_strict_offset_reset
    assert result.override_params == expected_override_params


@pytest.mark.parametrize(
    "sink_name, steps_config, expected_bootstrap_servers, expected_override_params",
    [
        pytest.param(
            "my_sink",
            {
                "my_sink": {
                    "bootstrap_servers": ["localhost:9092"],
                }
            },
            ["localhost:9092"],
            {},
            id="no_override_params",
        ),
        pytest.param(
            "my_sink",
            {
                "my_sink": {
                    "bootstrap_servers": ["broker1:9092", "broker2:9092"],
                    "override_params": {"acks": "all", "retries": "3"},
                }
            },
            ["broker1:9092", "broker2:9092"],
            {"acks": "all", "retries": "3"},
            id="override_params_pass_through",
        ),
    ],
)
def test_build_kafka_producer_config(
    sink_name: str,
    steps_config: Mapping[str, StepConfig],
    expected_bootstrap_servers: Sequence[str],
    expected_override_params: Mapping[str, str],
) -> None:
    """Test build_kafka_producer_config correctly builds PyKafkaProducerConfig."""
    result = build_kafka_producer_config(sink=sink_name, steps_config=steps_config)

    assert result is not None
    assert list(result.bootstrap_servers) == expected_bootstrap_servers
    assert result.override_params == expected_override_params
