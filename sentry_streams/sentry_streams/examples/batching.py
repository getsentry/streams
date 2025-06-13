from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, streaming_source
from sentry_streams.pipeline.chain import (
    BatchParser,
    Serializer,
    StreamSink,
)

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
parsed_batch = pipeline.apply("mybatch", Batch(batch_size=2)).apply(
    "batch_parser", BatchParser(msg_type=IngestMetric)
)

parsed_batch.apply("serializer", Serializer()).sink(
    "mysink", StreamSink(stream_name="transformed-events")
)
