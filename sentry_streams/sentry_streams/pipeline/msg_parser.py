from typing import Any, TypeVar

from sentry_kafka_schemas.codecs import Codec

from sentry_streams.pipeline.chain import Message

T = TypeVar("T")


def json_parser(msg: Message[bytes]) -> Message[Any]:

    schema: Codec[Any] = msg.schema
    payload = msg.payload

    decoded = schema.decode(payload, True)

    return Message(schema, decoded)


def json_serializer(msg: Message[Any]) -> bytes:

    schema: Codec[Any] = msg.schema
    payload = msg.payload

    encoded = schema.encode(payload)

    return encoded
