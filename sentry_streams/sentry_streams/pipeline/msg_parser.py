import json
from typing import Any, MutableMapping

from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.codecs import Codec

from sentry_streams.pipeline.message import Message, PyRawMessage

# TODO: Push the following to docs
# Standard message decoders and encoders live here
# These are used in the defintions of Parser() and Serializer() steps, see chain/

CODECS: MutableMapping[str, Codec[Any]] = {}


def msg_parser(msg: PyRawMessage) -> Any:
    stream_schema = msg.schema
    payload = msg.payload

    assert (
        stream_schema is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    try:
        codec = CODECS.get(stream_schema, get_codec(stream_schema))
    except Exception:
        raise ValueError(f"Kafka topic {stream_schema} has no associated schema")

    decoded = codec.decode(payload, True)

    return decoded


def msg_serializer(msg: Message[Any]) -> bytes:
    payload = msg.payload

    return json.dumps(payload).encode("utf-8")
