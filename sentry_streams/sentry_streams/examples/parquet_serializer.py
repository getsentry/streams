from typing import MutableSequence, Optional, Sequence, Tuple

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import (
    Batch,
    BatchParser,
    ParquetSerializer,
    StreamSink,
    streaming_source,
)
from sentry_streams.pipeline.datatypes import (
    Field,
    Int64,
    List,
    String,
    Struct,
)
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import Map

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)


# Extract bytes from the batched tuples
def extract_bytes_from_batch(
    msg: Message[MutableSequence[Tuple[bytes, Optional[str]]]]
) -> Sequence[bytes]:
    return [item[0] for item in msg.payload]


# Convert Sequence to MutableSequence for ParquetSerializer
def sequence_to_mutable_sequence(
    msg: Message[Sequence[IngestMetric]],
) -> MutableSequence[IngestMetric]:
    return list(msg.payload)


# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
parsed_batch = (
    pipeline.apply(Batch("mybatch", batch_size=2))
    .apply(Map("extract_bytes", function=extract_bytes_from_batch))
    .apply(BatchParser[IngestMetric]("batch_parser"))
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
serializer = ParquetSerializer[IngestMetric]("serializer", schema)
parsed_batch.apply(Map("to_mutable", function=sequence_to_mutable_sequence)).apply(serializer).sink(
    StreamSink[bytes]("mysink", stream_name="transformed-events")
)
