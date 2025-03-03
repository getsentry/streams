from sentry_streams.pipeline import (
    KafkaSink,
    KafkaSource,
    Pipeline,
    Reduce,
)
from sentry_streams.user_functions.function_template import Accumulator
from sentry_streams.window import TumblingWindow


class BatchBuilder(Accumulator[str, list[str], str]):
    """
    Takes str input and accumulates them into a batch array.
    Joins back into a string to produce onto a Kafka topic.
    """

    def create(self) -> list[str]:
        return []

    def add(self, acc: list[str], value: str) -> list[str]:
        acc.append(value)

        return acc

    def get_output(self, acc: list[str]) -> str:
        return " ".join(acc)

    def merge(self, acc1: list[str], acc2: list[str]) -> list[str]:
        return acc1 + acc2


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
    aggregate_fn=BatchBuilder(),
)

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
