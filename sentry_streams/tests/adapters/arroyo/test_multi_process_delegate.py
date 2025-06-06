from datetime import datetime
from typing import Any

from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.multi_process_delegate import (
    mapped_msg_to_rust,
    process_message,
    rust_to_arroyo_msg,
)
from sentry_streams.pipeline.message import (
    Message,
    PyMessage,
)
from sentry_streams.rust_streams import PyAnyMessage


def test_process_message() -> None:
    def transformer(msg: Message[bytes]) -> str:
        return f"transformed {msg.payload.decode('utf-8')}"

    msg = ArroyoMessage(
        Value(
            PyMessage(
                payload="foo".encode(), headers=[("h", "v".encode())], timestamp=123, schema="s"
            ),
            {Partition(Topic("topic1"), 0): 0},
        )
    )

    result = process_message(transformer, msg)

    assert result == PyMessage(
        "transformed foo", headers=[("h", "v".encode())], timestamp=123, schema="s"
    )


def test_mapped_msg_none() -> None:
    committable = {
        Partition(Topic("t"), 1): 42,
        Partition(Topic("t2"), 2): 99,
    }
    assert mapped_msg_to_rust(ArroyoMessage(Value(FilteredPayload(), committable))) is None


def test_mapped_msg_payload() -> None:
    committable = {
        Partition(Topic("t"), 1): 42,
        Partition(Topic("t2"), 2): 99,
    }
    payload = PyMessage("msg", headers=[("h", "v".encode())], timestamp=123, schema="s")
    msg = ArroyoMessage(Value(payload, committable, datetime.now()))
    result = mapped_msg_to_rust(msg)
    assert isinstance(result, tuple)
    ret_message, ret_committable = result
    assert ret_message.payload == "msg"
    assert ("t", 1) in ret_committable
    assert ("t2", 2) in ret_committable
    assert ret_committable[("t", 1)] == 42
    assert ret_committable[("t2", 2)] == 99


def test_rust_to_arroyo_msg_with_pyanymessage() -> None:
    committable = {("topic", 0): 123}
    message = PyAnyMessage("payload", headers=[("h", "v".encode())], timestamp=123, schema="s")
    arroyo_msg: ArroyoMessage[Message[Any]] = rust_to_arroyo_msg(message, committable)
    assert isinstance(arroyo_msg, ArroyoMessage)
    assert isinstance(arroyo_msg.payload, PyMessage)

    assert arroyo_msg.payload.payload == "payload"
    assert arroyo_msg.payload.headers == [("h", "v".encode())]
    assert arroyo_msg.payload.timestamp == 123
    assert arroyo_msg.payload.schema == "s"
    assert Partition(Topic("topic"), 0) in arroyo_msg.committable
    assert arroyo_msg.committable[Partition(Topic("topic"), 0)] == 123
