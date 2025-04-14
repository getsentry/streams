from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline.chain import Message


def json_parser(msg: bytes) -> Message[Mapping[str, Any]]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        new_msg: Message[Mapping[str, Any]] = Message({"type": "invalid"})
        return new_msg

    cast(Mapping[str, Any], parsed)
    new_msg = Message(parsed)
    return new_msg


def json_serializer(msg: Message[Mapping[str, Any]]) -> Any:
    return dumps(msg.payload)
