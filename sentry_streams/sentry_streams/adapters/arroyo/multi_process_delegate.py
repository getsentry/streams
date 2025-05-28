from __future__ import annotations

from datetime import datetime
from functools import partial
from typing import Callable, Generic, Tuple, TypeVar, Union, cast

from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
    RunTaskWithMultiprocessing,
)
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage
from arroyo.types import Partition, Topic, Value

from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    Committable,
    OutputRetriever,
    RustOperatorFactory,
)
from sentry_streams.pipeline.message import (
    Message,
    PyMessage,
    PyRawMessage,
    RustMessage,
)
from sentry_streams.pipeline.pipeline import (
    Map,
)
from sentry_streams.rust_streams import PyAnyMessage, RawMessage

# Payload of the Streaming Message that goes in the strategy
TMapIn = TypeVar("TMapIn")
# Payload of the Streaming Message that goes in the strategy
TMapOut = TypeVar("TMapOut")
TStrategyIn = TypeVar("TStrategyIn")
TStrategyOut = TypeVar("TStrategyOut")


def process_message(
    function: Callable[[Message[TMapIn]], TMapOut], msg: ArroyoMessage[Message[TMapIn]]
) -> Message[TMapOut]:
    in_payload = msg.payload
    ret = function(msg.payload)
    if isinstance(ret, bytes):
        # If `ret`` is bytes then function is Callable[Message[TMapIn], bytes].
        # Thus TMapOut = bytes.
        return cast(
            Message[TMapOut],
            PyRawMessage(
                payload=ret,
                headers=in_payload.headers,
                timestamp=in_payload.timestamp,
                schema=in_payload.schema,
            ),
        )

    return PyMessage(
        payload=ret,
        headers=in_payload.headers,
        timestamp=in_payload.timestamp,
        schema=in_payload.schema,
    )


def mapped_msg_to_rust(
    message: ArroyoMessage[Union[FilteredPayload, Message[TMapOut]]],
) -> Tuple[RustMessage, Committable] | None:
    if isinstance(message.payload, FilteredPayload):
        return None
    else:
        committable = {
            (partition.topic.name, partition.index): offset
            for partition, offset in message.committable.items()
        }

        return (message.payload.to_inner(), committable)


def rust_to_arroyo_msg(
    message: Message[TMapIn], committable: Committable
) -> ArroyoMessage[Message[TMapIn]]:
    arroyo_committable = {
        Partition(Topic(partition[0]), partition[1]): offset
        for partition, offset in committable.items()
    }
    if isinstance(message, PyAnyMessage):
        to_send = PyMessage(message.payload, message.headers, message.timestamp, message.schema)
    elif isinstance(message, RawMessage):
        to_send = PyRawMessage(message.payload, message.headers, message.timestamp, message.schema)

    msg = ArroyoMessage(
        Value(
            # TODO: Stop creating a `RoutedValue` and make the Reduce strategy
            # accept `Message` directly.
            to_send,
            arroyo_committable,
            datetime.fromtimestamp(message.timestamp) if message.timestamp else None,
        )
    )
    return msg


class MultiprocessDelegateFactory(RustOperatorFactory[TMapIn], Generic[TMapIn, TMapOut]):
    """
    Creates a delegate to build the RunTaskInMultiprocessing.
    This is the class to provide to the Rust runtime.
    """

    def __init__(
        self,
        step: Map,
        max_batch_size: int,
        max_batch_time: float,
        pool: MultiprocessingPool,
        input_block_size: int | None = None,
        output_block_size: int | None = None,
        max_input_block_size: int | None = None,
        max_output_block_size: int | None = None,
        prefetch_batches: bool = False,
    ) -> None:
        super().__init__()
        self.__step = step
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__pool = pool
        self.__input_block_size = input_block_size
        self.__output_block_size = output_block_size
        self.__max_input_block_size = max_input_block_size
        self.__max_output_block_size = max_output_block_size
        self.__prefetch_batches = prefetch_batches

    def build(
        self,
    ) -> ArroyoStrategyDelegate[
        TMapIn, FilteredPayload | Message[TMapIn], FilteredPayload | Message[TMapOut]
    ]:
        retriever = OutputRetriever[Union[FilteredPayload, Message[TMapOut]]](mapped_msg_to_rust)

        processor = RunTaskWithMultiprocessing(
            partial(process_message, self.__step.resolved_function),
            next_step=retriever,
            max_batch_size=self.__max_batch_size,
            max_batch_time=self.__max_batch_time,
            pool=self.__pool,
            input_block_size=self.__input_block_size,
            output_block_size=self.__output_block_size,
            max_input_block_size=self.__max_input_block_size,
            max_output_block_size=self.__max_output_block_size,
            prefetch_batches=self.__prefetch_batches,
        )

        ret = ArroyoStrategyDelegate(processor, rust_to_arroyo_msg, retriever)
        return ret
