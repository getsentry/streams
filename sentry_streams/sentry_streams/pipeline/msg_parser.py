from typing import Any

from sentry_streams.pipeline.message import Message

# TODO: Push the following to docs
# Standard message decoders and encoders live here
# Pass these into Parser() and Serializer() steps, see examples/


def msg_parser(msg: Message[bytes]) -> Message[Any]:
    codec = msg.schema
    payload = msg.payload

    assert (
        codec is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    decoded = codec.decode(payload, True)

    return Message(decoded, codec)


def msg_serializer(msg: Message[Any]) -> bytes:
    codec = msg.schema
    payload = msg.payload

    assert codec is not None

    encoded = codec.encode(payload, False)

    return encoded
