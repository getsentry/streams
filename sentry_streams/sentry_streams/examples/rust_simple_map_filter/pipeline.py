"""
Rust version of simple_map_filter.py example
"""

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Filter, Map, Parser, Serializer, streaming_source
from sentry_streams.pipeline.chain import StreamSink

# Import the compiled Rust functions
try:
    from metrics_rust_transforms import RustFilterEvents, RustTransformMsg
except ImportError as e:
    raise ImportError(
        "Rust extension 'metrics_rust_transforms' not found. "
        "You must build it first:\n"
        "  cd rust_transforms\n"
        "  maturin develop\n"
        f"Original error: {e}"
    ) from e

# Same pipeline structure as simple_map_filter.py, but with Rust functions
# that will be called directly without Python overhead
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="ingest-metrics",
    )
    .apply(
        "parser",
        Parser(
            msg_type=IngestMetric,
        ),
    )
    # This filter will run in native Rust with zero Python overhead
    .apply("filter", Filter(function=RustFilterEvents()))
    # This transform will run in native Rust with zero Python overhead
    .apply("transform", Map(function=RustTransformMsg()))
    .apply("serializer", Serializer())
    .sink("mysink", StreamSink(stream_name="transformed-events"))
)
