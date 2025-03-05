import json

from sentry_streams.examples.billing_buffer import OutcomesBuffer
from sentry_streams.pipeline import (
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Reduce,
)
from sentry_streams.window import TumblingWindow

Outcome = dict[str, str]


def build_outcome(value: str) -> Outcome:

    d: Outcome = json.loads(value)

    return d


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
reduce_window = TumblingWindow(window_size=3)

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=OutcomesBuffer,
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
