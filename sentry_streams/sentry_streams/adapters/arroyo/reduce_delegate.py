from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Generic, Tuple, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.reduce import build_arroyo_windowed_reduce
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    Committable,
    OutputRetriever,
    RustOperatorFactory,
)
from sentry_streams.pipeline.message import Message, PyMessage, RustMessage
from sentry_streams.pipeline.pipeline import Reduce

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


def rust_msg_to_arroyo_reduce(
    message: Message[TIn], committable: Committable
) -> ArroyoMessage[RoutedValue]:
    arroyo_committable = {
        Partition(Topic(partition[0]), partition[1]): offset
        for partition, offset in committable.items()
    }
    msg = ArroyoMessage(
        Value(
            # TODO: Stop creating a `RoutedValue` and make the Reduce strategy
            # accept `Message` directly.
            RoutedValue(Route(source="dummy", waypoints=[]), message),
            arroyo_committable,
            datetime.fromtimestamp(message.timestamp) if message.timestamp else None,
        )
    )
    return msg


def reduced_msg_to_rust(
    message: ArroyoMessage[Union[FilteredPayload, TIn]],
) -> Tuple[RustMessage, Committable] | None:
    if isinstance(message.payload, FilteredPayload):
        return None
    else:
        if isinstance(message.payload, RoutedValue):
            payload: Any = message.payload.payload
        else:
            payload = message.payload

        timestamp = message.timestamp.timestamp() if message.timestamp is not None else time.time()
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

        return (msg.to_inner(), committable)


TStrategyIn = TypeVar("TStrategyIn")
TStrategyOut = TypeVar("TStrategyOut")


class ReduceDelegateFactory(RustOperatorFactory[TIn], Generic[TIn, TStrategyOut]):
    """
    Creates a `ReduceDelegate`. This is the class to provide to the Rust runtime.
    """

    def __init__(self, step: Reduce[Any, Any, Any]) -> None:
        super().__init__()
        self.__step = step

    def build(
        self,
    ) -> ArroyoStrategyDelegate[
        TIn, Union[FilteredPayload, RoutedValue], Union[FilteredPayload, TStrategyOut]
    ]:
        retriever = OutputRetriever[Union[FilteredPayload, TStrategyOut]](reduced_msg_to_rust)
        route = Route(source="dummy", waypoints=[])

        # Need a cast because `build_arroyo_windowed_reduce` has the wrong type hint.
        # It uses the same parameter TPayload for input and output.
        reducer = cast(
            ProcessingStrategy[Union[FilteredPayload, RoutedValue]],
            build_arroyo_windowed_reduce(
                self.__step.windowing,
                self.__step.aggregate_fn,
                retriever,
                route,
            ),
        )

        return ArroyoStrategyDelegate(reducer, rust_msg_to_arroyo_reduce, retriever)
