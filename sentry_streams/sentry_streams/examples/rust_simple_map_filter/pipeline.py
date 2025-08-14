"""
Rust version of simple_map_filter.py example
"""

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Parser,
    Serializer,
    StreamSink,
    streaming_source,
)

# Import the compiled Rust functions
try:
    from metrics_rust_transforms import (
        RustFilterEvents,
        RustTransformMsg,
    )
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
pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

(
    pipeline.apply(Parser[IngestMetric]("parser"))
    # This filter will run in native Rust with zero Python overhead
    .apply(Filter("filter", function=RustFilterEvents()))
    # This transform will run in native Rust with zero Python overhead
    .apply(Map("transform", function=RustTransformMsg()))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-events"))
)
