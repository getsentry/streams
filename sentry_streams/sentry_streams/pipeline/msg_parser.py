import json
from typing import Any

from google.protobuf.message import Message as ProtoMessage

from sentry_streams.pipeline.message import Message, MessageSchema

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


def msg_serializer(msg: Message[Any], schema_type: MessageSchema) -> bytes:
    payload = msg.payload

    if schema_type is MessageSchema.PROTOBUF:
        assert isinstance(payload, ProtoMessage)
        return payload.SerializeToString()
    elif schema_type is MessageSchema.JSON:
        return json.dumps(payload).encode("utf-8")
    else:
        raise Exception(f"Unknown codec / message schema type {schema_type}")
