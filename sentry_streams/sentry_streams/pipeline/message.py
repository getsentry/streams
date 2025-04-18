from __future__ import annotations

from typing import (
    Any,
    Generic,
    MutableMapping,
    Optional,
    TypeVar,
)

from sentry_kafka_schemas.codecs import Codec

TIn = TypeVar("TIn")


# a message with a generic payload
class Message(Generic[TIn]):
    payload: TIn
    schema: Optional[Codec[Any]]
    additional: Optional[MutableMapping[str, Any]]

    def __init__(self, payload: Any, schema: Optional[Codec[Any]] = None) -> None:
        self.payload = payload
        self.schema = schema

    def replace_payload(self, p: TIn) -> None:
        self.payload = p
