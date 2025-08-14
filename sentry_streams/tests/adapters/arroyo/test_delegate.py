from typing import Union

import pytest
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import FilteredPayload, Partition, Topic

from sentry_streams.adapters.arroyo.multi_process_delegate import (
    mapped_msg_to_rust,
    rust_to_arroyo_msg,
)
from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    Committable,
    OutputRetriever,
    SingleMessageOperatorDelegate,
)
from sentry_streams.pipeline.message import (
    Message,
    PyMessage,
    RustMessage,
    rust_msg_equals,
)
from sentry_streams.rust_streams import PyAnyMessage
from tests.adapters.arroyo.helpers.delegate_helpers import (
    assert_equal_batches,
    str_transformer,
)
from tests.adapters.arroyo.helpers.message_helpers import (
    build_committable,
    build_rust_msg,
    build_watermark,
)


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


def test_arroyo_delegate_sends_right_watermark() -> None:
    retriever = OutputRetriever[Union[FilteredPayload, Message[str]]](mapped_msg_to_rust)

    delegate = ArroyoStrategyDelegate(
        RunTask(str_transformer, retriever), rust_to_arroyo_msg, retriever
    )

    delegate.submit(*build_rust_msg("payload", 0, build_committable(1, 42)))

    delegate.submit(*build_watermark(build_committable(1, 42), timestamp=0))

    # second watermark has a higher offset
    delegate.submit(*build_watermark(build_committable(1, 43), timestamp=0))

    ret = list(delegate.poll())

    expected = [
        build_rust_msg("transformed payload", 0, build_committable(1, 42)),
        build_watermark(
            build_committable(1, 42),
            timestamp=0,
        ),
    ]
    assert_equal_batches(
        ret,
        expected,
    )


def test_arroyo_delegate_globs_watermarks() -> None:
    retriever = OutputRetriever[Union[FilteredPayload, Message[str]]](mapped_msg_to_rust)

    delegate = ArroyoStrategyDelegate(
        RunTask(str_transformer, retriever), rust_to_arroyo_msg, retriever
    )

    delegate.submit(*build_watermark(build_committable(3, 100), timestamp=0))
    delegate.submit(*build_watermark(build_committable(4, 100), timestamp=0))

    delegate.submit(*build_rust_msg("payload", 0, {("test_topic", 0): 101}))
    # casting to consume the generator
    list(delegate.poll())

    delegate.submit(*build_rust_msg("payload", 0, {("test_topic", 1): 200}))
    # casting to consume the generator
    list(delegate.poll())

    delegate.submit(*build_rust_msg("payload", 0, {("test_topic", 2): 300}))
    ret = list(delegate.poll())
    expected = [
        build_rust_msg("transformed payload", 0, {("test_topic", 2): 300}),
        build_watermark(
            build_committable(3, 100),
            timestamp=0,
        ),
    ]
    assert_equal_batches(
        ret,
        expected,
    )
