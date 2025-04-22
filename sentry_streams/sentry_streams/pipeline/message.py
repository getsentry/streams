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


# A message with a generic payload
class Message(Generic[TIn]):
    payload: TIn
    schema: Optional[
        Codec[Any]
    ]  # The schema of incoming messages. This is optional so Messages can be flexibly initialized in any part of the pipeline. We may want to change this down the road.
    additional: Optional[MutableMapping[str, Any]]

    def __init__(self, payload: Any, schema: Optional[Codec[Any]] = None) -> None:
        self.payload = payload
        self.schema = schema

    def replace_payload(self, p: TIn) -> None:
        self.payload = p
