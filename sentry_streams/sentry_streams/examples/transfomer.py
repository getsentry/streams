import logging
from json import JSONDecodeError, loads
from typing import Any, Mapping, MutableSequence, Self, cast

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

logger = logging.getLogger(__name__)

# The simplest possible pipeline.
# - reads from Kafka
# - parses the event
# - filters the event based on an attribute
# - serializes the event into json
# - produces the event on Kafka


def parse(msg: str) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


class TransformerBatch(Accumulator[Any, Any]):
    """
    Takes a generic input format, and batches into a generic batch representation
    with the same input type. Returns this batch representation.

    The data type of the elements remains the same through this operation.
    """

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


pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
)

parser = Map(name="parser", ctx=pipeline, inputs=[source], function=parse)

filter = Filter(
    name="myfilter", ctx=pipeline, inputs=[parser], function=lambda msg: msg["type"] == "event"
)

reduce_window = SlidingWindow(window_size=3, window_slide=2)

reduce = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[filter],
    window=reduce_window,
    aggregate_func=TransformerBatch,
)


# jsonify = Map(name="serializer", ctx=pipeline, inputs=[filter], function=lambda msg: dumps(msg))

sink = StreamSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    stream_name="transformed-events",
)
