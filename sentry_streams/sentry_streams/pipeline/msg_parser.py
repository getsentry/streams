import json
import logging
from typing import Any

from sentry_streams.pipeline.message import Message

logger = logging.getLogger(__name__)


def json_parser(msg: Message[bytes]) -> Message[Any]:
    schema = msg.schema
    assert schema is not None

    payload = msg.payload

    decoded = schema.decode(payload, True)

    return Message(decoded, schema)


def json_serializer(msg: Message[Any]) -> bytes:
    return json.dumps(msg.payload).encode("utf-8")
