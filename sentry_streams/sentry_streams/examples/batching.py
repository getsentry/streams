from typing import Self

from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.pipeline import (
    KafkaSink,
    KafkaSource,
    Pipeline,
    Reduce,
)
from sentry_streams.pipeline.window import TumblingWindow


# TODO: Build a generic BatchBuilder provided by the platform. Provides a JSON string output.
class BatchBuilder(Accumulator[str, str]):
    """
    Takes str input and accumulates them into a batch array.
    Joins back into a string to produce onto a Kafka topic.
    """

    def __init__(self) -> None:
        self.batch: list[str] = []

    def add(self, value: str) -> Self:
        self.batch.append(value)

        return self

    def get_value(self) -> str:
        return " ".join(self.batch)

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

# A sample window.
# Windows are assigned 4 elements.
reduce_window = TumblingWindow(window_size=4)

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[source],
    windowing=reduce_window,
    aggregate_fn=BatchBuilder,
)

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
