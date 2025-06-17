import pickle
from typing import Type, Union

import pytest

from sentry_streams.pipeline.message import (
    PyMessage,
    PyRawMessage,
    rust_msg_equals,
)
from sentry_streams.rust_streams import PyAnyMessage, RawMessage


@pytest.mark.parametrize(
    "message, expected_rust_type",
    [
        (
            PyRawMessage(
                payload=b"payload",
                headers=[("header1", b"test")],
                timestamp=10.0,
                schema="schema",
            ),
            RawMessage,
        ),
        (
            PyMessage[str](
                payload="payload",
                headers=[("header1", b"test")],
                timestamp=10.0,
                schema="schema",
            ),
            PyAnyMessage,
        ),
    ],
)
def test_message_access(
    message: Union[PyRawMessage, PyMessage[str]],
    expected_rust_type: Union[Type[PyAnyMessage], Type[RawMessage]],
) -> None:
    assert message.headers == [("header1", "test".encode())]
    assert message.timestamp == 10.0
    assert message.schema == "schema"

    assert str(message) == repr(message)

    assert isinstance(message.to_inner(), expected_rust_type)

    copy = message.deepcopy()
    assert id(copy) != id(message)
    assert copy.payload == message.payload
    assert copy.headers == message.headers
    assert copy.timestamp == message.timestamp
    assert copy.schema == message.schema

    assert rust_msg_equals(copy.to_inner(), message.to_inner())

    reloaded = pickle.loads(pickle.dumps(message))

    assert reloaded.payload == message.payload
    assert reloaded.headers == message.headers
    assert reloaded.timestamp == message.timestamp
    assert reloaded.schema == message.schema

    assert rust_msg_equals(reloaded.to_inner(), message.to_inner())
