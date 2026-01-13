"""
Example pipeline demonstrating DevNullSink for benchmarking and testing.

This pipeline reads from a Kafka topic, applies a simple transformation,
and discards the results using DevNullSink. Useful for:
- Benchmarking pipeline throughput
- Testing pipeline logic without persistence overhead
- Simulating sink behavior with configurable delays
"""

from typing import Any, Mapping

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import transform_msg
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Map,
    Parser,
    streaming_source,
)

# Create the pipeline
pipeline = streaming_source(name="benchmark_source", stream_name="ingest-metrics")

(
    pipeline.apply(Parser[IngestMetric]("parser"))
    .apply(Map("transform", function=transform_msg))
    .sink(
        DevNullSink[Mapping[str, Any]](
            name="devnull",
            batch_size=500,  # Flush every 500 messages
            batch_time_ms=5000.0,  # Or every 5 seconds
            average_sleep_time_ms=500.0,  # Average 500ms delay to simulate I/O
            max_sleep_time_ms=6000.0,  # Max 6 seconds delay
        )
    )
)
