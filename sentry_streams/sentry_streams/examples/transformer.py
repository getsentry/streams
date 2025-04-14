from datetime import timedelta
from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, MutableSequence, Self, cast

from sentry_streams.pipeline import Filter, streaming_source
from sentry_streams.pipeline.chain import Parser, Reducer
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.msg_parser import json_parser, json_serializer
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

# chain1 = streaming_source(
#     name="myinput",
#     stream_name="events",
# )

# chain2 = chain1.apply("parser", Parser(deserializer=json_parser))
# chain3 = chain2.apply("myfilter", Filter(function=filter_not_event))
# chain4 = chain3.apply("myreduce", Reducer(reduce_window, TransformerBatch))
# chain5 = chain4.apply("serializer", Map(function=serialize_msg))
# chain6 = chain5.sink(
#     "kafkasink2", stream_name="transformed-events"
# )  # flush the batches to the Sink


pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("parser", Parser(deserializer=json_parser))
    .apply("myfilter", Filter(function=filter_not_event))
    .apply("myreduce", Reducer(reduce_window, TransformerBatch))
    .sink(
        "kafkasink2",
        stream_name="transformed-events",
        serializer=json_serializer,
    )  # flush the batches to the Sink
)
