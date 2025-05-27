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
    """ """

    def __init__(
        self,
    ) -> None:
        self.__pending_messages: MutableSequence[Tuple[Message[TIn], Committable]] = []

    def submit(self, message: ArroyoMessage[Union[FilteredPayload, TIn]]) -> None:
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
        This method is a placeholder and should be implemented to return
        the actual output messages.
        """
        ret = self.__pending_messages
        self.__pending_messages = []
        return ret


class ReduceDelegateFactory(RustOperatorFactory[TIn, TOut], Generic[TIn, TOut]):

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
