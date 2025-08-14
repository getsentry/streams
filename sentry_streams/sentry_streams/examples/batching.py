# echo '{"org_id":420,"project_id":420,"name":"s:sessions/user@none","tags":{"sdk":"raven-node/2.6.3","environment":"production","release":"sentry-test@1.0.0"},"timestamp":11111111111,"type":"s","retention_days":90,"value":[1617781333]}' | kcat -P -b 127.0.0.1:9092 -t ingest-metrics

from typing import MutableSequence, Optional, Sequence, Tuple

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, streaming_source
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    BatchParser,
    Map,
    Serializer,
    StreamSink,
)

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)


# Extract bytes from the batched tuples
def extract_bytes_from_batch(
    msg: Message[MutableSequence[Tuple[bytes, Optional[str]]]]
) -> Sequence[bytes]:
    return [item[0] for item in msg.payload]


# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
parsed_batch = (
    pipeline.apply(Batch("mybatch", batch_size=2))
    .apply(Map("extract_bytes", function=extract_bytes_from_batch))
    .apply(BatchParser[IngestMetric]("batch_parser"))
)

parsed_batch.apply(Serializer("serializer")).sink(
    StreamSink("mysink", stream_name="transformed-events")
)
