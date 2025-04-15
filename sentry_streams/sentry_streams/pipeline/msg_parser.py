from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast


def json_parser(msg: bytes) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def json_serializer(msg: Mapping[str, Any]) -> str:
    return dumps(msg)
