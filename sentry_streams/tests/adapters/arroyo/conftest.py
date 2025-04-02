from datetime import timedelta
from typing import Any, Callable, MutableSequence, Self

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Filter,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import SlidingWindow


@pytest.fixture
def broker() -> LocalBroker[KafkaPayload]:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, MockedClock())
    broker.create_topic(Topic("logical-events"), 1)
    broker.create_topic(Topic("transformed-events"), 1)
    return broker


class TransformerBatch(Accumulator[Any, Any]):
    def __init__(self) -> None:
        self.batch: MutableSequence[Any] = []

    def add(self, value: Any) -> Self:
        self.batch.append(value)

        return self

    def get_value(self) -> Any:
        return "".join(self.batch)

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


@pytest.fixture
def transformer() -> Callable[[], TransformerBatch]:
    return TransformerBatch


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = Pipeline()
    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )
    decoder = Map(
        name="decoder",
        ctx=pipeline,
        inputs=[source],
        function=lambda msg: msg.decode("utf-8"),
    )
    filter = Filter(
        name="myfilter", ctx=pipeline, inputs=[decoder], function=lambda msg: msg == "go_ahead"
    )
    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[filter],
        function=lambda msg: msg + "_mapped",
    )
    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        stream_name="transformed-events",
    )

    return pipeline


@pytest.fixture
def reduce_pipeline() -> Pipeline:
    pipeline = Pipeline()
    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )
    decoder = Map(
        name="decoder",
        ctx=pipeline,
        inputs=[source],
        function=lambda msg: msg.decode("utf-8"),
    )
    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[decoder],
        function=lambda msg: msg + "_mapped",
    )
    reduce_window = SlidingWindow(
        window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2)
    )
    reduce = Aggregate(
        name="myreduce",
        ctx=pipeline,
        inputs=[map],
        window=reduce_window,
        aggregate_func=TransformerBatch,
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[reduce],
        stream_name="transformed-events",
    )

    return pipeline
