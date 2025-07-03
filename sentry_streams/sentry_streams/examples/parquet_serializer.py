from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, streaming_source
from sentry_streams.pipeline.chain import (
    BatchParser,
    ParquetSerializer,
    StreamSink,
)
from sentry_streams.pipeline.datatypes import (
    Field,
    Int64,
    List,
    String,
    Struct,
)

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
parsed_batch = pipeline.apply("mybatch", Batch(batch_size=2)).apply(
    "batch_parser", BatchParser(msg_type=IngestMetric)
)

schema = {
    "org_id": Int64(),
    "project_id": Int64(),
    "name": String(),
    "tags": Struct(
        [
            Field("sdk", String()),
            Field("environment", String()),
            Field("release", String()),
        ]
    ),
    "timestamp": Int64(),
    "type": String(),
    "retention_days": Int64(),
    "value": List(Int64()),
}
serializer = ParquetSerializer(schema)
parsed_batch.apply("serializer", serializer).sink(
    "mysink", StreamSink(stream_name="transformed-events")
)
