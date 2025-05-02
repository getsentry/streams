import json
from typing import Any

from sentry_streams.pipeline.message import Message

# TODO: Push the following to docs
# Standard message decoders and encoders live here
# These are used in the defintions of Parser() and Serializer() steps, see chain/


def msg_parser(msg: Message[bytes]) -> Any:
    codec = msg.schema
    payload = msg.payload

    assert (
        codec is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    decoded = codec.decode(payload, True)

    return decoded


def msg_serializer(msg: Message[Any]) -> bytes:
    payload = msg.payload

    return json.dumps(payload).encode("utf-8")
