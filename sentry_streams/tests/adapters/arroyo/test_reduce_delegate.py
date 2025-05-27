from datetime import datetime
from typing import MutableSequence, Optional, Sequence, Tuple, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.reduce_delegate import (
    OutputRetriever,
    ReduceDelegate,
    ReduceDelegateFactory,
)
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.rust_step import Committable
from sentry_streams.pipeline.chain import ExtensibleChain
from sentry_streams.pipeline.message import Message, PyMessage
from sentry_streams.pipeline.pipeline import Batch


def test_retriever() -> None:
    retriever = OutputRetriever[str]()

    timestamp = datetime.now()
    retriever.submit(
        ArroyoMessage(
            Value("payload", {Partition(Topic("test_topic"), 0): 100}, timestamp),
        )
    )
    retriever.submit(
        ArroyoMessage(
            Value("payload2", {Partition(Topic("test_topic"), 0): 200}, timestamp),
        )
    )
    output = list(retriever.fetch())

    assert len(output) == 2

    assert output[0][0] == PyMessage(
        payload="payload",
        headers=[],
        timestamp=timestamp.timestamp(),
        schema=None,
    )
    assert output[0][1] == {("test_topic", 0): 100}

    assert output[1][0] == PyMessage(
        payload="payload2",
        headers=[],
        timestamp=timestamp.timestamp(),
        schema=None,
    )
    assert output[1][1] == {("test_topic", 0): 200}


class FakeReducer(ProcessingStrategy[Union[FilteredPayload, RoutedValue]]):
    def __init__(self, next: ProcessingStrategy[Sequence[Message[str]]]) -> None:
        self.__messages: MutableSequence[Message[str]] = []
        self.__next = next

    def submit(self, message: ArroyoMessage[Union[FilteredPayload, RoutedValue]]) -> None:
        if isinstance(message.payload, FilteredPayload):
            return
        self.__messages.append(message.payload.payload)

    def _flush(self) -> None:
        self.__next.submit(
            ArroyoMessage(
                Value(
                    self.__messages,
                    {Partition(Topic("test_topic"), 0): 400},
                    timestamp=datetime.fromtimestamp(self.__messages[0].timestamp),
                ),
            )
        )
        self.__messages = []

    def poll(self) -> None:
        if len(self.__messages) > 2:
            # Simulate a batch of messages being processed
            self._flush()

    def join(self, timeout: Optional[float] = None) -> None:
        self._flush()
        self.__next.join(timeout)

    def close(self) -> None:
        self.__next.close()

    def terminate(self) -> None:
        self.__next.terminate()


def build_msg(payload: str, timestamp: float, offset: int) -> Tuple[PyMessage[str], Committable]:
    return (
        PyMessage(
            payload=payload,
            headers=[],
            timestamp=timestamp,
            schema=None,
        ),
        {("test_topic", 0): offset},
    )


def assert_equal_batches(
    batch1: Sequence[Tuple[Message[Sequence[Message[str]]], Committable]],
    batch2: Sequence[Tuple[Message[Sequence[Message[str]]], Committable]],
) -> None:
    assert len(batch1) == len(batch2)
    for i, msg1 in enumerate(batch1):
        msg2 = batch2[i]
        assert msg1[0].payload == msg2[0].payload, f"Payload mismatch at index {i}"
        assert msg1[1] == msg2[1], f"Committable mismatch at index {i}"


def test_reduce_poll() -> None:
    retriever = OutputRetriever[Sequence[Message[str]]]()
    reducer = FakeReducer(retriever)

    delegate = ReduceDelegate[str, Sequence[Message[str]]](
        reducer,
        retriever,
        Route("test_route", []),
    )

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, 100),
    )
    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message2", timestamp, 200),
    )
    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message3", timestamp, 300),
    )

    # Poll to trigger processing
    batch = list(delegate.poll())
    assert_equal_batches(
        batch,
        [
            (
                PyMessage(
                    payload=[
                        build_msg("message1", timestamp, 100)[0],
                        build_msg("message2", timestamp, 200)[0],
                        build_msg("message3", timestamp, 300)[0],
                    ],
                    headers=[],
                    timestamp=timestamp,
                    schema=None,
                ).to_inner(),
                {("test_topic", 0): 400},
            )
        ],
    )

    assert len(list(delegate.poll())) == 0
    assert len(list(retriever.fetch())) == 0


def test_flush() -> None:
    retriever = OutputRetriever[Sequence[Message[str]]]()
    reducer = FakeReducer(retriever)

    delegate = ReduceDelegate[str, Sequence[Message[str]]](
        reducer,
        retriever,
        Route("test_route", []),
    )

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, 100),
    )
    batch = list(delegate.flush())
    assert_equal_batches(
        batch,
        [
            (
                PyMessage(
                    payload=[
                        build_msg("message1", timestamp, 100)[0],
                    ],
                    headers=[],
                    timestamp=timestamp,
                    schema=None,
                ).to_inner(),
                {("test_topic", 0): 400},
            )
        ],
    )

    assert len(list(delegate.poll())) == 0


def test_reduce() -> None:
    pipeline: ExtensibleChain[Message[bytes]] = ExtensibleChain("root")
    factory = ReduceDelegateFactory[str, Sequence[str]](Batch("batch", pipeline, [], 2))
    delegate = factory.build()

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, 100),
    )
    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message2", timestamp, 200),
    )

    batch = list(delegate.poll())
    expected = [
        (
            PyMessage(
                payload=["message1", "message2"],
                headers=[],
                timestamp=timestamp,
                schema=None,
            ).to_inner(),
            {("test_topic", 0): 200},
        )
    ]
    assert len(batch) == len(expected)
    for i, msg1 in enumerate(batch):
        msg2 = expected[i]
        assert msg1[0].payload == msg2[0].payload, f"Payload mismatch at index {i}"
        assert msg1[1] == msg2[1], f"Committable mismatch at index {i}"
