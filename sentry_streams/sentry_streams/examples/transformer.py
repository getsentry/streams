from datetime import timedelta
from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, MutableSequence, Self, cast

from sentry_streams.pipeline import Filter, Map, streaming_source
from sentry_streams.pipeline.chain import Reducer
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.window import SlidingWindow

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


def filter_not_event(msg: Mapping[str, Any]) -> bool:
    return bool(msg["type"] == "event")


def serialize_msg(msg: Mapping[str, Any]) -> str:
    return dumps(msg)


class TransformerBatch(Accumulator[Any, Any]):

    def __init__(self) -> None:
        self.batch: MutableSequence[Any] = []

    def add(self, value: Any) -> Self:
        self.batch.append(value["test"])

        return self

    def get_value(self) -> Any:
        return "".join(self.batch)

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


reduce_window = SlidingWindow(window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2))

pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("mymap", Map(function=parse))
    .apply("myfilter", Filter(function=filter_not_event))
    .apply("myreduce", Reducer(reduce_window, TransformerBatch))
    .apply("serializer", Map(function=serialize_msg))
    .sink(
        "kafkasink2",
        stream_name="transformed-events",
    )  # flush the batches to the Sink
)
