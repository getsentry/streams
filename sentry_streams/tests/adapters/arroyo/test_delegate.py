import pytest
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import Partition, Topic

from sentry_streams.adapters.arroyo.rust_step import (
    Committable,
    SingleMessageOperatorDelegate,
)
from sentry_streams.pipeline.message import (
    PyMessage,
    RustMessage,
    rust_msg_equals,
)
from sentry_streams.rust_streams import PyAnyMessage


class SingleMessageTransformer(SingleMessageOperatorDelegate):
    def _process_message(self, msg: RustMessage, committable: Committable) -> RustMessage | None:
        if msg.payload == "process":
            return PyMessage("processed", msg.headers, msg.timestamp, msg.schema).to_inner()
        if msg.payload == "filter":
            return None
        else:
            partition, offset = next(iter(committable.items()))
            raise InvalidMessage(Partition(Topic(partition[0]), partition[1]), offset)


def test_rust_step() -> None:
    def make_msg(payload: str) -> RustMessage:
        return PyAnyMessage(
            payload=payload, headers=[("head", "val".encode())], timestamp=0, schema=None
        )

    step = SingleMessageTransformer()
    # Transform one message
    step.submit(make_msg("process"), {("topic", 0): 0})
    ret = step.poll()
    assert rust_msg_equals(list(ret)[0][0], make_msg("processed"))
    assert list(ret)[0][1] == {("topic", 0): 0}

    # The message is removed from the delegate after processing.
    ret = step.poll()
    assert ret == []
    # Filter one message
    step.submit(make_msg("filter"), {("topic", 0): 0})
    assert step.poll() == []
    # The message is removed and we accept another message
    step.submit(make_msg("process"), {("topic", 0): 0})
    # If we submit twice we reject the message
    with pytest.raises(MessageRejected):
        step.submit(make_msg("process"), {("topic", 0): 0})
    step.poll()
    # Submit and process an invalid message
    step.submit(make_msg("invalid"), {("topic", 0): 0})
    with pytest.raises(InvalidMessage):
        step.poll()
    # Test that flush processes the message as well.
    step.submit(make_msg("process"), {("topic", 0): 0})
    ret = step.flush(0)
    assert rust_msg_equals(list(ret)[0][0], make_msg("processed"))
    assert list(ret)[0][1] == {("topic", 0): 0}
