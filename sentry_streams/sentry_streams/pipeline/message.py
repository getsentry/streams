from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Generic, Optional, Sequence, Tuple, TypeVar, cast

from sentry_streams.rust_streams import PyAnyMessage, RawMessage

TPayload = TypeVar("TPayload")


class Message(ABC, Generic[TPayload]):
    """
    A generic class to represent multiple types of messages in the pipeline.
    Streaming Steps should access the message via this class.

    The actual Message classes are defined in Rust and are exported to Python
    via pyo3. This class and its subclasses wrap the Rust classes so that we
    can make the payload of the Rust classes generic. It is not possible to
    create, via pyo3, a class that is generic to Python nor using Rust generics.

    The other reason this class exists is to have a superclass for all message
    types in Python. In rust we have a rich enum.

    TODO: Find a way to avoid redeclaring all the message types in Python.
          we should be able to only redeclare the messages where we want to
          make payload a Python Generic.
    """

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

    @abstractmethod
    def to_inner(self) -> Any:
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


class PyMessage(Generic[TPayload], Message[TPayload]):
    """
    A wrapper for the Rust PyAnyMessage to make the payload generic.

    The payload of the inner rust class is `Any` for Python,
    but this class casts it to a Generic type.
    This does not offer the same guarantee as if the code was all
    in Python as the Rust code may not respect the type hint.
    By making this type generic we can still get a lot of guarantees
    by the type checker when wiring python primitives together as
    it can ensure primitives are compatible with each other.
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

    def __repr__(self) -> str:
        return f"PyMessage({self.inner.__repr__()})"

    def __str__(self) -> str:
        return self.inner.__str__()

    def to_inner(self) -> Any:
        return self.inner

    def deepcopy(self) -> PyMessage[TPayload]:
        return PyMessage(
            deepcopy(self.inner.payload),
            deepcopy(self.inner.headers),
            self.inner.timestamp,
            self.inner.schema,
        )


class PyRawMessage(Message[bytes]):
    """
    A wrapper for the Rust RawMessage so `RawMessage` extends `Messasge`.
    """

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

    def __repr__(self) -> str:
        return f"RawMessage({self.inner.__repr__()})"

    def __str__(self) -> str:
        return self.inner.__str__()

    def to_inner(self) -> Any:
        return self.inner

    def deepcopy(self) -> PyRawMessage:
        return PyRawMessage(
            deepcopy(self.inner.payload),
            deepcopy(self.inner.headers),
            self.inner.timestamp,
            self.inner.schema,
        )
