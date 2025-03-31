import json
from typing import Any

from sentry_streams.examples.billing_buffer import OutcomesBuffer
from sentry_streams.pipeline.function_template import KVAggregationBackend
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Map,
    Pipeline,
    StreamSource,
)
from sentry_streams.pipeline.window import TumblingWindow

Outcome = dict[str, str]


def build_outcome(value: str) -> Outcome:
    d: Outcome = json.loads(value)

    return d


# pipeline: special name
pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
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

reduce: Aggregate[int, Outcome, dict[Any, Any]] = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    window=reduce_window,
    aggregate_func=OutcomesBuffer,
    aggregate_backend=KVAggregationBackend(),  # NOTE: Provided by the platform
)
