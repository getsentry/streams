from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Generic, Optional, Sequence, Tuple, TypeVar, cast

from sentry_streams.rust_streams import PyAnyMessage, RawMessage

TPayload = TypeVar("TPayload")


class Message(ABC, Generic[TPayload]):
    @property
    @abstractmethod
    def payload(self) -> TPayload:
        raise NotImplementedError

    @property
    @abstractmethod
    def headers(self) -> Sequence[Tuple[str, bytes]]:
        raise NotImplementedError

    @property
    @abstractmethod
    def timestamp(self) -> float:
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def deepcopy(self) -> Message[TPayload]:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return False
        return (
            self.payload == other.payload
            and self.headers == other.headers
            and self.timestamp == other.timestamp
            and self.schema == other.schema
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"payload={self.payload!r}, "
            f"headers={self.headers!r}, "
            f"timestamp={self.timestamp!r}, "
            f"schema={self.schema!r})"
        )


class PyMessage(Generic[TPayload], Message[TPayload]):
    """
    A wrapper for the Rust PyAnyMessage to make the payload generic.
    We cannot export classes via PyO3 that are generic in Python.
    This exposes makes the Message generic.
    """

    def __init__(
        self,
        payload: TPayload,
        headers: Sequence[Tuple[str, bytes]],
        timestamp: float,
        schema: Optional[str] = None,
    ) -> None:
        self.inner = PyAnyMessage(payload, headers, timestamp, schema)

    @property
    def payload(self) -> TPayload:
        return cast(TPayload, self.inner.payload)

    @property
    def headers(self) -> Sequence[Tuple[str, bytes]]:
        return self.inner.headers

    @property
    def timestamp(self) -> float:
        return self.inner.timestamp

    @property
    def schema(self) -> str | None:
        return self.inner.schema

    def deepcopy(self) -> PyMessage[TPayload]:
        return PyMessage(
            deepcopy(self.inner.payload),
            deepcopy(self.inner.headers),
            self.inner.timestamp,
            self.inner.schema,
        )


class PyRawMessage(Message[bytes]):
    def __init__(
        self,
        payload: bytes,
        headers: Sequence[Tuple[str, bytes]],
        timestamp: float,
        schema: Optional[str] = None,
    ) -> None:
        self.inner = RawMessage(payload, headers, timestamp, schema)

    @property
    def payload(self) -> bytes:
        return self.inner.payload

    @property
    def headers(self) -> Sequence[Tuple[str, bytes]]:
        return self.inner.headers

    @property
    def timestamp(self) -> float:
        return self.inner.timestamp

    @property
    def schema(self) -> str | None:
        return self.inner.schema

    def deepcopy(self) -> PyRawMessage:
        return PyRawMessage(
            deepcopy(self.inner.payload),
            deepcopy(self.inner.headers),
            self.inner.timestamp,
            self.inner.schema,
        )
