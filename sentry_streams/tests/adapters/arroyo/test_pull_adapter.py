"""Tests for the PullBasedAdapter — verifies DSL steps map to the correct
PullOperator variants, and e2e Python → Rust pipeline execution."""

from sentry_streams.adapters.arroyo.pull_adapter import PullBasedAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.pipeline import (
    Batch,
    BatchParser,
    GCSSink,
    HeadersFilter,
    Map,
    Pipeline,
    streaming_source,
)
from sentry_streams.runner import iterate_edges
from sentry_streams.rust_streams import (
    PullConsumer,
    PullOperator,
    PullSourceConfig,
    PyTestMessage,
)


def _dummy_processor(msg):
    """Mock processor function for Map steps."""
    return msg


def _dummy_object_generator():
    """Mock GCS object name generator."""
    return "test-object.parquet"


def test_pull_adapter_items_span_pipeline():
    """Verify the adapter produces the correct PullOperator list
    for a pipeline shaped like items_span."""

    ITEM_TYPE_SPAN = 1

    pipeline: Pipeline[bytes] = (
        streaming_source(name="kafka", stream_name="snuba-items")
        .apply(
            HeadersFilter(
                name="span_filter",
                header_name="item_type",
                value=ITEM_TYPE_SPAN,
            )
        )
        .apply(Batch(name="batcher", batch_size=50000))
        .apply(Map(name="processor", function=_dummy_processor))
        .sink(
            GCSSink(
                name="gcs_sink",
                bucket="test-bucket",
                object_generator=_dummy_object_generator,
            )
        )
    )

    adapter = PullBasedAdapter.build(
        {
            "steps_config": {
                "kafka": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test-group",
                    "override_params": {},
                },
            },
        }
    )

    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Verify source was configured
    assert adapter._kafka_config is not None
    assert adapter._topic == "snuba-items"

    # Verify steps
    assert len(adapter._steps) == 3

    # Step 0: HeaderFilter
    step0 = adapter._steps[0]
    assert isinstance(step0, PullOperator)
    # PullOperator is a PyO3 enum — check the variant by accessing fields
    # PyO3 complex enums expose variant names differently, so we check
    # by verifying the attributes exist
    assert step0.header_name == "item_type"
    assert step0.expected_value == ITEM_TYPE_SPAN

    # Step 1: Batch
    step1 = adapter._steps[1]
    assert isinstance(step1, PullOperator)
    assert step1.max_batch_size == 50000

    # Step 2: PyCallable (Map)
    step2 = adapter._steps[2]
    assert isinstance(step2, PullOperator)
    assert step2.name == "processor"
    assert step2.callable is _dummy_processor

    # Verify sink
    assert adapter._sink is not None
    assert adapter._sink.bucket == "test-bucket"
    assert adapter._sink.object_generator is _dummy_object_generator


def test_pull_adapter_header_filter_only():
    """Verify a minimal pipeline with just a filter."""

    pipeline: Pipeline[bytes] = (
        streaming_source(name="src", stream_name="test-topic")
        .apply(
            HeadersFilter(
                name="my_filter",
                header_name="type",
                value=42,
            )
        )
        .sink(
            GCSSink(
                name="sink",
                bucket="bucket",
                object_generator=_dummy_object_generator,
            )
        )
    )

    adapter = PullBasedAdapter.build(
        {
            "steps_config": {
                "src": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test-group",
                },
            },
        }
    )

    iterate_edges(pipeline, RuntimeTranslator(adapter))

    assert len(adapter._steps) == 1
    assert adapter._steps[0].header_name == "type"
    assert adapter._steps[0].expected_value == 42
    assert adapter._sink is not None


def test_pull_adapter_batch_parser_converts_to_map():
    """BatchParser is a ComplexStep that converts to a Map via convert().
    Verify the adapter receives it as a PyCallable."""

    pipeline: Pipeline[bytes] = (
        streaming_source(name="src", stream_name="test-topic")
        .apply(Batch(name="batcher", batch_size=100))
        .apply(BatchParser[bytes]("parser"))
        .sink(
            GCSSink(
                name="sink",
                bucket="bucket",
                object_generator=_dummy_object_generator,
            )
        )
    )

    adapter = PullBasedAdapter.build(
        {
            "steps_config": {
                "src": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test-group",
                },
            },
        }
    )

    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Batch + BatchParser(converted to Map) = 2 steps
    assert len(adapter._steps) == 2

    # Step 0: Batch
    assert adapter._steps[0].max_batch_size == 100

    # Step 1: PyCallable (from BatchParser.convert() → Map)
    step1 = adapter._steps[1]
    assert step1.name == "parser"
    # The callable should be the batch_msg_parser function
    assert callable(step1.callable)


# ── E2E: Python → Rust pipeline execution ───────────────────────

# Shared capture list for e2e tests — populated by capturing Map steps.
_captured: list = []


def _capture_and_passthrough(msg):
    """Map function that captures msg.payload into _captured, then returns it."""
    _captured.append(msg.payload)
    return msg.payload


def test_pull_consumer_e2e_python_to_rust():
    """Full e2e: Python DSL → PullBasedAdapter → PullConsumer → stages → verify output.

    Pipeline: HeaderFilter → Batch(2) → PyCallable(capture) → no sink

    Uses a capturing Map step to verify data flows through the pipeline.
    No mock sink needed — the capture happens in a regular pipeline stage.
    """
    _captured.clear()

    pipeline: Pipeline[bytes] = (
        streaming_source(name="kafka", stream_name="test-topic")
        .apply(
            HeadersFilter(
                name="filter",
                header_name="item_type",
                value=1,
            )
        )
        .apply(Batch(name="batcher", batch_size=2))
        .apply(Map(name="capture", function=_capture_and_passthrough))
        .sink(
            GCSSink(
                name="gcs_sink",
                bucket="test-bucket",
                object_generator=_dummy_object_generator,
            )
        )
    )

    adapter = PullBasedAdapter.build(
        {
            "steps_config": {
                "kafka": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test-group",
                    "override_params": {},
                },
            },
        }
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Verify the adapter set schema on the PyCallable
    py_callable_step = adapter._steps[2]  # HeaderFilter, Batch, PyCallable
    assert py_callable_step.schema == "test-topic"

    source = PullSourceConfig.Test(
        messages=[
            PyTestMessage(payload=b"span-0", headers={"item_type": b"1"}),
            PyTestMessage(payload=b"span-1", headers={"item_type": b"1"}),
            PyTestMessage(payload=b"span-2", headers={"item_type": b"2"}),  # filtered out
            PyTestMessage(payload=b"span-3", headers={"item_type": b"1"}),
            PyTestMessage(payload=b"span-4", headers={"item_type": b"1"}),
        ]
    )

    consumer = PullConsumer(
        source=source,
        steps=adapter._steps,
        sink=None,  # no sink — capture step verifies output
    )
    consumer.run()

    # 4 matching messages, batch size 2 → 2 batches captured
    assert len(_captured) == 2, f"Expected 2 batches, got {len(_captured)}"


def test_pull_consumer_e2e_complex_steps():
    """E2E test with real ComplexStep conversions: BatchParser and ParquetSerializer.

    Pipeline: Batch(2) → BatchParser[TraceItem] → Map(extract_org_id) → ParquetSerializer → Map(capture)

    Tests that:
    - BatchParser.convert() produces a Map(batch_msg_parser) that works with Message wrapping
    - batch_msg_parser uses msg.schema to find the codec and parses protobuf bytes
    - ParquetSerializer.convert() produces a Map(serialize_to_parquet) that serializes to parquet
    - The full chain works end-to-end through our pull pipeline
    """
    from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem as TraceItemProto

    from sentry_streams.pipeline.datatypes import Uint64
    from sentry_streams.pipeline.pipeline import BatchParser, ParquetSerializer

    _captured.clear()

    def extract_org_id(msg):
        """Map function: Sequence[TraceItem] → list[dict]"""
        return [{"org_id": item.organization_id} for item in msg.payload]

    def capture_parquet(msg):
        """Capture parquet bytes output."""
        _captured.append(msg.payload)
        return msg.payload

    pipeline: Pipeline[bytes] = (
        streaming_source(name="kafka", stream_name="snuba-items")
        .apply(Batch(name="batcher", batch_size=2))
        .apply(BatchParser[TraceItemProto]("parser"))
        .apply(Map(name="processor", function=extract_org_id))
        .apply(
            ParquetSerializer(
                name="serializer",
                schema_fields={"org_id": Uint64()},
            )
        )
        .apply(Map(name="capture", function=capture_parquet))
        .sink(
            GCSSink(
                name="gcs_sink",
                bucket="test-bucket",
                object_generator=_dummy_object_generator,
            )
        )
    )

    adapter = PullBasedAdapter.build(
        {
            "steps_config": {
                "kafka": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test-group",
                    "override_params": {},
                },
            },
        }
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    item1 = TraceItemProto()
    item1.organization_id = 42
    item1.trace_id = b"0123456789abcdef"

    item2 = TraceItemProto()
    item2.organization_id = 99
    item2.trace_id = b"fedcba9876543210"

    item3 = TraceItemProto()
    item3.organization_id = 7
    item3.trace_id = b"aaaaaaaaaaaaaaaa"

    item4 = TraceItemProto()
    item4.organization_id = 123
    item4.trace_id = b"bbbbbbbbbbbbbbbb"

    source = PullSourceConfig.Test(
        messages=[
            PyTestMessage(payload=item1.SerializeToString(), headers={}),
            PyTestMessage(payload=item2.SerializeToString(), headers={}),
            PyTestMessage(payload=item3.SerializeToString(), headers={}),
            PyTestMessage(payload=item4.SerializeToString(), headers={}),
        ]
    )

    consumer = PullConsumer(
        source=source,
        steps=adapter._steps,
        sink=None,
    )
    consumer.run()

    # 4 messages, batch size 2 → 2 batches → 2 parquet outputs captured
    assert len(_captured) == 2, f"Expected 2 parquet outputs, got {len(_captured)}"

    # Verify the results are actual parquet bytes (magic number: PAR1)
    for i, result_bytes in enumerate(_captured):
        assert result_bytes[:4] == b"PAR1", f"Result {i} doesn't start with PAR1 magic"
