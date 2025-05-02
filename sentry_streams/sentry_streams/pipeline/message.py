from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    MutableSequence,
    Optional,
    Tuple,
    TypeVar,
)

from sentry_kafka_schemas.codecs import Codec

TIn = TypeVar("TIn")  # TODO: Consider naming this TPayload


# A message with a generic payload
@dataclass(frozen=True)
class Message(Generic[TIn]):
    payload: TIn
    headers: MutableSequence[Tuple[str, bytes]]
    timestamp: float
    schema: Optional[
        Codec[Any]
    ]  # The schema of incoming messages. This is optional so Messages can be flexibly initialized in any part of the pipeline. We may want to change this down the road.
    # TODO: Add support for an event timestamp
