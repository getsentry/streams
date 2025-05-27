from __future__ import annotations

import time
from datetime import datetime
from typing import (
    Any,
    Generic,
    Iterable,
    MutableSequence,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.reduce import build_arroyo_windowed_reduce
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.rust_step import (
    Committable,
    RustOperatorDelegate,
    RustOperatorFactory,
)
from sentry_streams.pipeline.message import Message, PyMessage
from sentry_streams.pipeline.pipeline import Reduce

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


class OutputRetriever(ProcessingStrategy[Union[FilteredPayload, TIn]]):
    """
    This is an Arroyo strategy to be wired to another strategy used inside
    a `RustOperatorDelegate`. This strategy collects the result and return it to the
    Rust code.

    Arroyo strategies are provided the following step and are expected to
    hand the result directly to it. This does not work for `RustOperatorDelegate`
    which is expected to return the result as return value of poll and flush.

    In order to wrap an existing Arroyo strategy in a `RustOperatorDelegate` we
    need to provide an instance of this class to the existing strategy to
    collect the results and send it them back to Rust as `poll` return value.
    """

    def __init__(
        self,
    ) -> None:
        self.__pending_messages: MutableSequence[Tuple[Message[TIn], Committable]] = []

    def submit(self, message: ArroyoMessage[Union[FilteredPayload, TIn]]) -> None:
        """
        Accumulates messages provided by the previous step in the consumer.

        Different types of reducers can provide RoutedValues or bare aggregated
        data. So this class has to support both.
        Messages are turned into `PyMessage` and stored in this format.
        """
        if isinstance(message.payload, FilteredPayload):
            return
        else:
            if isinstance(message.payload, RoutedValue):
                payload: Any = message.payload.payload
            else:
                payload = message.payload

            timestamp = (
                message.timestamp.timestamp() if message.timestamp is not None else time.time()
            )
            msg = PyMessage(
                payload=payload,
                headers=[],
                timestamp=timestamp,
                schema=None,
            )

            committable = {
                (partition.topic.name, partition.index): offset
                for partition, offset in message.committable.items()
            }

            self.__pending_messages.append((msg, committable))

    def poll(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def fetch(self) -> Iterable[Tuple[Message[TIn], Committable]]:
        """
        Fetches the output messages from the processing strategy.
        """
        ret = self.__pending_messages
        self.__pending_messages = []
        return ret


class ReduceDelegateFactory(RustOperatorFactory[TIn, TOut], Generic[TIn, TOut]):
    """
    Creates a `ReduceDelegate`. This is the class to provide to the Rust runtime.
    """

    def __init__(self, step: Reduce[Any, Any, Any]) -> None:
        super().__init__()
        self.__step = step

    def build(self) -> ReduceDelegate[TIn, TOut]:
        retriever = OutputRetriever[TOut]()
        route = Route(source="dummy", waypoints=[])

        return ReduceDelegate(
            build_arroyo_windowed_reduce(
                self.__step.windowing,
                self.__step.aggregate_fn,
                retriever,
                route,
            ),
            retriever,
            route,
        )


class ReduceDelegate(RustOperatorDelegate[TIn, TOut], Generic[TIn, TOut]):
    """
    Wraps the various types of Python Reduce steps to be used by the
    Rust runtime. Eventually we will move the reduce logic itself to Rust.

    It receives the messages to add to the accumulator with the `submit` method.
    These messages are directly forwarded to the Reduce strategy after
    turning them into `RoutedValue`.

    Extracts the results from `OutputRetriever` after polling on the Reduce
    strategy.
    """

    def __init__(
        self,
        inner: ProcessingStrategy[Union[FilteredPayload, Any]],
        output_retriever: OutputRetriever[TOut],
        route: Route,
    ) -> None:
        super().__init__()
        self.__inner = inner
        self.__retriever = output_retriever
        self.__route = route

    def submit(self, message: Message[TIn], committable: Committable) -> None:
        arroyo_committable = {
            Partition(Topic(partition[0]), partition[1]): offset
            for partition, offset in committable.items()
        }
        msg = ArroyoMessage(
            Value(
                # TODO: Stop creating a `RoutedValue` and make the Reduce strategy
                # accept `Message` directly.
                RoutedValue(self.__route, message),
                arroyo_committable,
                datetime.fromtimestamp(message.timestamp) if message.timestamp else None,
            )
        )
        self.__inner.submit(msg)

    def poll(self) -> Iterable[Tuple[Message[TOut], Committable]]:
        self.__inner.poll()
        ret = [(msg.to_inner(), committable) for msg, committable in self.__retriever.fetch()]
        return ret

    def flush(self, timeout: float | None = None) -> Iterable[Tuple[Message[TOut], Committable]]:
        self.__inner.join(timeout)
        ret = [(msg.to_inner(), committable) for msg, committable in self.__retriever.fetch()]
        self.__inner.close()
        return ret
