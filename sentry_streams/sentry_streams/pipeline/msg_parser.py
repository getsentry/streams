import json
from typing import Any, MutableMapping, Sequence

from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.codecs import Codec

from sentry_streams.pipeline.message import Message, PyRawMessage

# TODO: Push the following to docs
# Standard message decoders and encoders live here
# These are used in the defintions of Parser() and Serializer() steps, see chain/

CODECS: MutableMapping[str, Codec[Any]] = {}

def _get_codec(stream_schema: str) -> Codec:
    assert (
        stream_schema is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    try:
        codec = CODECS.get(stream_schema, get_codec(stream_schema))
    except Exception:
        raise ValueError(f"Kafka topic {stream_schema} has no associated schema")
    return codec

def msg_parser(msg: PyRawMessage) -> Any:
    stream_schema = msg.schema
    payload = msg.payload
    codec = _get_codec(stream_schema)
    decoded = codec.decode(payload, True)

    return decoded

def batch_msg_parser(msg: Message[Sequence[bytes]]) -> Sequence[Any]:
    print("2")
    payloads = msg.payload
    stream_schema = msg.schema
    print("3")
    codec = _get_codec(stream_schema)
    print("4")
    return [codec.decode(payload, True) for payload in payloads]

def msg_serializer(msg: Message[Any]) -> bytes:
    payload = msg.payload

    return json.dumps(payload).encode("utf-8")
