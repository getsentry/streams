from datetime import datetime
from typing import Tuple, Union, cast

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
from sentry_streams.pipeline.message import Message, PyMessage, RustMessage


def rust_to_arroyo_msg(
    message: RustMessage, committable: Committable
) -> ArroyoMessage[Message[str]]:
    arroyo_committable = {
        Partition(Topic(partition[0]), partition[1]): offset
        for partition, offset in committable.items()
    }
    arroyo_msg = cast(
        Message[str], PyMessage(message.payload, message.headers, message.timestamp, message.schema)
    )
    return ArroyoMessage(
        Value(
            arroyo_msg,
            arroyo_committable,
            datetime.fromtimestamp(message.timestamp) if message.timestamp else None,
        )
    )


def arroyo_msg_to_rust(
    message: ArroyoMessage[Union[FilteredPayload, Message[str]]]
) -> Tuple[RustMessage, Committable] | None:
    if isinstance(message.payload, FilteredPayload):
        return None
    committable = {
        (partition.topic.name, partition.index): offset
        for partition, offset in message.committable.items()
    }
    return (message.payload.to_inner(), committable)


def noop(
    msg: ArroyoMessage[Union[FilteredPayload, Message[str]]]
) -> Union[FilteredPayload, Message[str]]:
    return msg.payload


class TestDelegateFactory(RustOperatorFactory):
    """
    A delegate used in rust_streams tests for the PythonAdapter step.
    """

    def build(
        self,
    ) -> ArroyoStrategyDelegate[FilteredPayload | Message[str], FilteredPayload | Message[str]]:
        retriever = OutputRetriever(out_transformer=arroyo_msg_to_rust)
        inner = RunTask(noop, retriever)
        return ArroyoStrategyDelegate(inner, rust_to_arroyo_msg, retriever)
