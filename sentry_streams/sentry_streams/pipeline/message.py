from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import (
    Any,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from sentry_streams.rust_streams import PyAnyMessage, PyWatermark, RawMessage

TPayload = TypeVar("TPayload")

# Represents the Rust message types which are actually processed by delegate steps
RustMessage = Union[PyAnyMessage, RawMessage]
# Represents the Rust message types which can be sent into a delegate step
PipelineMessage = Union[RustMessage, PyWatermark]


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
    def to_inner(self) -> RustMessage:
        raise NotImplementedError

    @abstractmethod
    def size(self) -> int | None:
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

    The Rust ``PyAnyMessage`` is built on first :meth:`to_inner` and then cached,
    so normal use of the Python wrapper avoids pyo3 allocation until a step
    needs the inner type. Pickle serializes only the Python fields; the cache is
    cleared on unpickle so the inner is recreated when needed.
    """

    def __init__(
        self,
        payload: TPayload,
        headers: Sequence[Tuple[str, bytes]],
        timestamp: float,
        schema: Optional[str] = None,
    ) -> None:
        self._payload: TPayload = payload
        self._headers: Sequence[Tuple[str, bytes]] = headers
        self._timestamp = timestamp
        self._schema = schema
        self._cached_inner: PyAnyMessage | None = None

    @property
    def payload(self) -> TPayload:
        return self._payload

    @property
    def headers(self) -> Sequence[Tuple[str, bytes]]:
        return self._headers

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def schema(self) -> str | None:
        return self._schema

    def size(self) -> int | None:
        if isinstance(self._payload, (str, bytes)):
            return len(self._payload)
        return None

    def __repr__(self) -> str:
        return (
            f"PyMessage(PyAnyMessage(payload={self._payload!r}, "
            f"headers={self._headers!r}, timestamp={self._timestamp}, "
            f"schema={self._schema!r}))"
        )

    def __str__(self) -> str:
        return repr(self)

    def to_inner(self) -> PyAnyMessage:
        if self._cached_inner is None:
            self._cached_inner = PyAnyMessage(
                self._payload, self._headers, self._timestamp, self._schema
            )
        return self._cached_inner

    def deepcopy(self) -> PyMessage[TPayload]:
        return PyMessage(
            deepcopy(self._payload),
            deepcopy(self._headers),
            self._timestamp,
            self._schema,
        )

    def __getstate__(self) -> Mapping[str, Any]:
        return {
            "payload": self.payload,
            "headers": self.headers,
            "timestamp": self.timestamp,
            "schema": self.schema,
        }

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self._payload = state["payload"]
        self._headers = state["headers"]
        self._timestamp = state["timestamp"]
        self._schema = state.get("schema")
        self._cached_inner = None


class PyRawMessage(Message[bytes]):
    """
    A wrapper for the Rust RawMessage so ``RawMessage`` extends ``Message``.

    The Rust ``RawMessage`` is built on first :meth:`to_inner` and then cached,
    so constructing or copying this wrapper avoids pyo3 work until a delegate
    needs the inner type. Pickle serializes only the Python fields; the cache is
    cleared on unpickle so the inner is recreated when needed.
    """

    def __init__(
        self,
        payload: bytes,
        headers: Sequence[Tuple[str, bytes]],
        timestamp: float,
        schema: Optional[str] = None,
    ) -> None:
        self._payload = payload
        self._headers: Sequence[Tuple[str, bytes]] = headers
        self._timestamp = timestamp
        self._schema = schema
        self._cached_inner: RawMessage | None = None

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def headers(self) -> Sequence[Tuple[str, bytes]]:
        return self._headers

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def schema(self) -> str | None:
        return self._schema

    def size(self) -> int | None:
        return len(self._payload)

    def __repr__(self) -> str:
        return (
            f"RawMessage(RawMessage(payload={self._payload!r}, "
            f"headers={self._headers!r}, timestamp={self._timestamp}, "
            f"schema={self._schema!r}))"
        )

    def __str__(self) -> str:
        return repr(self)

    def to_inner(self) -> RawMessage:
        if self._cached_inner is None:
            self._cached_inner = RawMessage(
                self._payload, self._headers, self._timestamp, self._schema
            )
        return self._cached_inner

    def deepcopy(self) -> PyRawMessage:
        return PyRawMessage(
            deepcopy(self._payload),
            deepcopy(self._headers),
            self._timestamp,
            self._schema,
        )

    def __getstate__(self) -> Mapping[str, Any]:
        return {
            "payload": self.payload,
            "headers": self.headers,
            "timestamp": self.timestamp,
            "schema": self.schema,
        }

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self._payload = state["payload"]
        self._headers = state["headers"]
        self._timestamp = state["timestamp"]
        self._schema = state.get("schema")
        self._cached_inner = None


def rust_msg_equals(msg: RustMessage, other: RustMessage) -> bool:
    """
    PyAnyMessage/RawMessage are exposed by Rust and do not have an __eq__ method
    as of now. That would require delegating equality to python anyway
    as the payload is a PyAny
    """
    return (
        msg.payload == other.payload
        and msg.headers == other.headers
        and msg.timestamp == other.timestamp
        and msg.schema == other.schema
    )
