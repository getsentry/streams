from datetime import datetime
from typing import Tuple, Union

from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    Committable,
    OutputRetriever,
    RustOperatorFactory,
)
from sentry_streams.pipeline.message import RustMessage


def rust_to_arroyo_msg(
    message: RustMessage, committable: Committable
) -> ArroyoMessage[RustMessage]:
    arroyo_committable = {
        Partition(Topic(partition[0]), partition[1]): offset
        for partition, offset in committable.items()
    }
    return ArroyoMessage(
        Value(
            message,
            arroyo_committable,
            datetime.fromtimestamp(message.timestamp) if message.timestamp else None,
        )
    )


def arroyo_msg_to_rust(
    message: ArroyoMessage[Union[FilteredPayload, RustMessage]],
) -> Tuple[RustMessage, Committable] | None:
    if isinstance(message.payload, FilteredPayload):
        return None
    committable = {
        (partition.topic.name, partition.index): offset
        for partition, offset in message.committable.items()
    }
    return (message.payload, committable)


def noop(
    msg: ArroyoMessage[Union[FilteredPayload, RustMessage]],
) -> Union[FilteredPayload, RustMessage]:
    return msg.payload


class TestDelegateFactory(RustOperatorFactory):
    """
    A delegate used in rust_streams tests for the PythonAdapter step.
    """

    def build(
        self,
    ) -> ArroyoStrategyDelegate[FilteredPayload | RustMessage, FilteredPayload | RustMessage]:
        retriever = OutputRetriever(out_transformer=arroyo_msg_to_rust)
        inner = RunTask(noop, retriever)
        return ArroyoStrategyDelegate(inner, rust_to_arroyo_msg, retriever)
