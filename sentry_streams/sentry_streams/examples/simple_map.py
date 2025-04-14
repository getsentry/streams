from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline import Map, streaming_source


def parse(msg: str) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def transform_msg(msg: Mapping[str, Any]) -> Mapping[str, Any]:
    return {**msg, "transformed": True}


def serialize_msg(msg: Mapping[str, Any]) -> bytes:
    ret = dumps(msg).encode()
    print(f"PROCESSING {msg}")
    return ret


def print_msg(msg: Any) -> Any:
    print(f"PROCESSING {msg}")
    return msg


pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("mymap", Map(function=parse))
    .apply("transform", Map(function=transform_msg))
    .apply("serializer", Map(function=serialize_msg))
    .sink("mysink", "transformed-events")
)
