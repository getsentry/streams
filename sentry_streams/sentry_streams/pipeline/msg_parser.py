import json
from typing import Any

from sentry_streams.pipeline.message import Message

# Standard message decoders and encoders live here
# Choose between transports like JSON, protobuf


def json_parser(msg: Message[bytes]) -> Message[Any]:
    schema = msg.schema
    assert (
        schema is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    payload = msg.payload

    decoded = schema.decode(payload, True)

    return Message(decoded, schema)


def json_serializer(msg: Message[Any]) -> bytes:
    return json.dumps(msg.payload).encode("utf-8")
