from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline import Filter, Map, streaming_source
from sentry_streams.pipeline.message import Message


def parse(msg: Message[bytes]) -> Mapping[str, Any]:
    try:
        parsed = loads(msg.payload)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def transform_msg(msg: Message[Mapping[str, Any]]) -> Mapping[str, Any]:
    return {**msg.payload, "transformed": True}


def filter_events(msg: Message[Mapping[str, Any]]) -> bool:
    return "event" in msg.payload


def serialize_msg(msg: Message[Mapping[str, Any]]) -> bytes:
    ret = dumps(msg.payload).encode()
    print(f"PROCESSING {msg}")
    return ret


# A pipline with a few transformations
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("mymap", Map(function=parse))
    .apply("filter", Filter(function=filter_events))
    .apply("transform", Map(function=transform_msg))
    .apply("serializer", Map(function=serialize_msg))
    .sink("mysink", "transformed-events")
)
