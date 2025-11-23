from typing import Any, Mapping

from sentry_kafka_schemas.schema_types.processed_profiles_v1 import (
    _Root as ProcessedProfile,
)

from sentry_streams.pipeline.message import Message

num = 0


def transform_msg(msg: Message[ProcessedProfile]) -> Mapping[str, Any]:
    global num
    num += 1
    return {**msg.payload, "transformed": True}
