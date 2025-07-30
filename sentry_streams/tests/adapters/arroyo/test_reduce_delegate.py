from datetime import datetime
from typing import (
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.reduce_delegate import (
    ReduceDelegateFactory,
    reduced_msg_to_rust,
    rust_msg_to_arroyo_reduce,
)
from sentry_streams.adapters.arroyo.routes import RoutedValue
from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    Committable,
    OutputRetriever,
)
from sentry_streams.pipeline.message import (
    Message,
    PipelineMessage,
    PyMessage,
    RustMessage,
    pipeline_msg_equals,
)
from sentry_streams.pipeline.pipeline import Batch
from sentry_streams.rust_streams import PyWatermark

TStrategyOut = TypeVar("TStrategyOut")


def test_retriever() -> None:
    retriever = OutputRetriever[Union[FilteredPayload, str]](reduced_msg_to_rust)

    timestamp = datetime.now()
    retriever.submit(
        ArroyoMessage(
            Value("payload", {Partition(Topic("topic0"), 0): 100}, timestamp),
        )
    )
    retriever.submit(
        ArroyoMessage(
            Value("payload2", {Partition(Topic("topic0"), 0): 200}, timestamp),
        )
    )
    output = list(retriever.fetch())

    assert len(output) == 2

    assert pipeline_msg_equals(
        output[0][0],
        PyMessage(
            payload="payload",
            headers=[],
            timestamp=timestamp.timestamp(),
            schema=None,
        ).to_inner(),
    )
    assert output[0][1] == {("topic0", 0): 100}

    assert pipeline_msg_equals(
        output[1][0],
        PyMessage(
            payload="payload2",
            headers=[],
            timestamp=timestamp.timestamp(),
            schema=None,
        ).to_inner(),
    )
    assert output[1][1] == {("topic0", 0): 200}


class FakeReducer(ProcessingStrategy[Union[FilteredPayload, RoutedValue]]):
    def __init__(self, next: ProcessingStrategy[Sequence[Message[str]]]) -> None:
        self.__messages: MutableSequence[Message[str]] = []
        self.__committable: dict[Partition, int] = {}
        self.__next = next

    def submit(self, message: ArroyoMessage[Union[FilteredPayload, RoutedValue]]) -> None:
        if isinstance(message.payload, FilteredPayload):
            return
        self.__messages.append(message.payload.payload)
        self.__committable.update(message.committable)

    def _flush(self) -> None:
        self.__next.submit(
            ArroyoMessage(
                Value(
                    self.__messages,
                    self.__committable,
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


def build_msg(
    payload: str, timestamp: float, committable: Committable
) -> Tuple[PipelineMessage, Committable]:
    msg, committable = build_py_msg(payload, timestamp, committable)
    return (msg.to_inner(), committable)


def build_py_msg(
    payload: str, timestamp: float, committable: Committable
) -> Tuple[PyMessage[str], Committable]:
    return (
        PyMessage(
            payload=payload,
            headers=[],
            timestamp=timestamp,
            schema="ingest-metrics",
        ),
        committable,
    )


def build_watermark(committable: Committable, timestamp: int) -> Tuple[RustMessage, Committable]:
    return (
        PyWatermark(
            committable,
            timestamp,
        ),
        committable,
    )


def build_committable(num_partitions: int, starting_offset: int) -> Committable:
    return {(f"topic{i}", i): starting_offset + i for i in range(num_partitions)}


def assert_equal_batches(
    batch1: Sequence[Tuple[RustMessage, Committable]],
    batch2: Sequence[Tuple[RustMessage, Committable]],
) -> None:
    assert len(batch1) == len(batch2)
    for i, msg1 in enumerate(batch1):
        msg2 = batch2[i]
        assert msg1[0].payload == msg2[0].payload, f"Payload mismatch at index {i}"
        assert msg1[1] == msg2[1], f"Committable mismatch at index {i}"


def test_reduce_poll() -> None:
    retriever = OutputRetriever[Sequence[Message[str]]](reduced_msg_to_rust)
    reducer = FakeReducer(retriever)

    delegate = ArroyoStrategyDelegate[RoutedValue, Sequence[Message[str]]](
        reducer,
        rust_msg_to_arroyo_reduce,
        retriever,
    )

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, {("topic0", 0): 100}),
    )
    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message2", timestamp, {("topic1", 1): 200}),
    )
    assert len(list(delegate.poll())) == 0

    # Submitted watermark does not trigger processing, is recorded in watermark list
    delegate.submit(*build_watermark(build_committable(3, 100), 0))
    assert len(list(delegate.poll())) == 0
    assert len(delegate.watermarks()) == 1

    delegate.submit(
        *build_msg("message3", timestamp, {("topic2", 2): 300}),
    )

    # Poll to trigger processing
    batch = list(delegate.poll())

    expected = [
        (
            PyMessage(
                payload=[
                    build_py_msg("message1", timestamp, {("topic0", 0): 100})[0],
                    build_py_msg("message2", timestamp, {("topic1", 1): 200})[0],
                    build_py_msg("message3", timestamp, {("topic2", 2): 300})[0],
                ],
                headers=[],
                timestamp=timestamp,
                schema=None,
            ).to_inner(),
            {
                ("topic0", 0): 100,
                ("topic1", 1): 200,
                ("topic2", 2): 300,
            },
        ),
        build_watermark(build_committable(3, 100), 0),
    ]
    assert_equal_batches(
        batch,
        expected,
    )

    assert len(list(delegate.poll())) == 0
    assert len(list(retriever.fetch())) == 0


def test_flush() -> None:
    retriever = OutputRetriever[Sequence[Message[str]]](reduced_msg_to_rust)
    reducer = FakeReducer(retriever)

    delegate = ArroyoStrategyDelegate[RoutedValue, Sequence[Message[str]]](
        reducer,
        rust_msg_to_arroyo_reduce,
        retriever,
    )

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, {("topic0", 0): 100}),
    )
    batch = list(delegate.flush())
    assert_equal_batches(
        batch,
        [
            (
                PyMessage(
                    payload=[
                        build_py_msg("message1", timestamp, {("topic0", 0): 100})[0],
                    ],
                    headers=[],
                    timestamp=timestamp,
                    schema=None,
                ).to_inner(),
                {("topic0", 0): 100},
            )
        ],
    )

    assert len(list(delegate.poll())) == 0


def test_reduce() -> None:
    factory = ReduceDelegateFactory[Sequence[str]](Batch("batch", 4))
    delegate = factory.build()

    timestamp = datetime.now().timestamp()
    # Simulate the reducer processing messages
    delegate.submit(
        *build_msg("message1", timestamp, {("topic0", 0): 100}),
    )
    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message2", timestamp, {("topic0", 0): 200}),
    )

    assert len(list(delegate.poll())) == 0

    delegate.submit(
        *build_msg("message3", timestamp, {("topic0", 0): 300}),
    )

    delegate.submit(
        *build_msg("message4", timestamp, {("topic0", 0): 400}),
    )

    batch = list(delegate.poll())
    expected = [
        (
            PyMessage(
                payload=["message1", "message2", "message3", "message4"],
                headers=[],
                timestamp=timestamp,
                schema="ingest-metrics",
            ).to_inner(),
            {("topic0", 0): 400},
        )
    ]
    assert len(batch) == len(expected)
    for i, msg1 in enumerate(batch):
        msg2 = expected[i]
        assert msg1[0].payload == msg2[0].payload, f"Payload mismatch at index {i}"
        if not isinstance(msg1[0], PyWatermark) or not isinstance(msg1[0], PyWatermark):
            assert msg1[0].schema == msg2[0].schema, "Missing schema after batch"
        assert msg1[1] == msg2[1], f"Committable mismatch at index {i}"
