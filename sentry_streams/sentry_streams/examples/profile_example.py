from typing import MutableSequence, Optional, Sequence, Tuple

from sentry_kafka_schemas.schema_types.processed_profiles_v1 import (
    _Root as ProcessedProfile,
)


from sentry_streams.pipeline import Batch, BatchParser
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    Serializer,
    StreamSink,
    streaming_source,
)

pipeline = streaming_source(name="myinput", stream_name="processed-profiles")


def extract_bytes_from_batch(
    msg: Message[MutableSequence[Tuple[bytes, Optional[str]]]],
) -> Sequence[bytes]:
    return [item[0] for item in msg.payload]


ret = (
    pipeline.apply(Batch("mybatch", batch_size=10))
    # .apply(Map("extract_bytes", function=extract_bytes_from_batch))
    .apply(BatchParser[ProcessedProfile]("batch_parser"))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-profiles"))
)
