import pytest
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import Partition, Topic

from sentry_streams.adapters.arroyo.rust_step import (
    Committable,
    SingleMessageOperatorDelegate,
)
from sentry_streams.pipeline.message import Message, PyMessage


class SingleMessageTransformer(SingleMessageOperatorDelegate[str, str]):
    def _process_message(self, msg: Message[str], committable: Committable) -> Message[str] | None:
        if msg.payload == "process":
            return PyMessage("processed", msg.headers, msg.timestamp, msg.schema)
        if msg.payload == "filter":
            return None
        else:
            partition, offset = next(iter(committable.items()))
            raise InvalidMessage(Partition(Topic(partition[0]), partition[1]), offset)


def test_rust_step() -> None:
    def make_msg(payload: str) -> Message[str]:
        return PyMessage(
            payload=payload, headers=[("head", "val".encode())], timestamp=0, schema=None
        )

    step = SingleMessageTransformer()

    # Transform one message
    step.submit(make_msg("process"), {("topic", 0): 0})
    ret = step.poll()
    assert ret == [
        (make_msg("processed"), {("topic", 0): 0}),
    ]

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
    assert ret == [(make_msg("processed"), {("topic", 0): 0})]
