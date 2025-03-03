import json
from typing import Optional

from sentry_streams.examples.billing_buffer import PendingBuffer
from sentry_streams.pipeline import (
    KafkaSource,
    Map,
    Pipeline,
    Reduce,
)
from sentry_streams.user_functions.function_template import (
    Accumulator,
    AggregationBackend,
)
from sentry_streams.window import TumblingWindow

Outcome = dict[str, str]


def build_outcome(value: str) -> Outcome:

    d: Outcome = json.loads(value)

    return d


class OutcomesBackend(AggregationBackend[PendingBuffer]):

    def __init__(self) -> None:
        self.storage_map: dict[str, int] = {"state": 0, "data_cat": 0}

    def flush_aggregate(self, aggregate: PendingBuffer) -> None:

        self.storage_map["state"] = aggregate.get_key("state")
        self.storage_map["data_cat"] = aggregate.get_key("data_cat")


class OutcomesBuffer(Accumulator[Outcome, PendingBuffer, PendingBuffer]):

    def __init__(self, backend: Optional[OutcomesBackend] = None):
        self.backend = backend

    def create(self) -> PendingBuffer:
        buffer = PendingBuffer()
        return buffer

    def add(self, acc: PendingBuffer, value: Outcome) -> PendingBuffer:
        acc.add(value)

        return acc

    def get_output(self, acc: PendingBuffer) -> PendingBuffer:
        if self.backend:
            self.backend.flush_aggregate(acc)

        return acc

    def merge(self, acc1: PendingBuffer, acc2: PendingBuffer) -> PendingBuffer:

        first = acc1.map
        second = acc2.map

        new_dict = {
            "state": first["state"] + second["state"],
            "data_cat": first["data_cat"] + second["data_cat"],
        }

        new_buffer = PendingBuffer(new_dict)

        return new_buffer


# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[source],
    function=build_outcome,
)

# A sample window.
# Windows are assigned 3 elements.
# But aggregation can be triggered as soon as 2 elements are seen.
reduce_window = TumblingWindow(window_size=3)
agg_backend = OutcomesBackend()

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=OutcomesBuffer(agg_backend),
)
