import json
from typing import Self

from sentry_streams.examples.word_counter_fn import (
    GroupByWord,
)
from sentry_streams.pipeline import Filter, Map, Reducer, streaming_source
from sentry_streams.pipeline.chain import StreamSink
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.window import TumblingWindow


def simple_filter(value: Message[bytes]) -> bool:
    d = json.loads(value.payload)
    return True if "name" in d else False


def simple_map(value: Message[bytes]) -> tuple[str, int]:
    d = json.loads(value.payload)
    word: str = d.get("word", "null_word")

    return (word, 1)


class WordCounter(Accumulator[Message[tuple[str, int]], str]):

    def __init__(self) -> None:
        self.tup = ("", 0)

    def add(self, value: Message[tuple[str, int]]) -> Self:
        self.tup = (value.payload[0], self.tup[1] + value.payload[1])

        return self

    def get_value(self) -> str:
        return f"{self.tup[0]} {self.tup[1]}"

    def merge(self, other: Self) -> Self:
        first = self.tup[0] + other.tup[0]
        second = self.tup[1] + other.tup[1]

        self.tup = (first, second)

        return self


# A sample window.
# Windows are assigned 3 elements.
# TODO: Get the parameters for window in pipeline configuration.
reduce_window = TumblingWindow(window_size=3)

# pipeline: special name
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply(
        "myfilter",
        Filter(
            function=simple_filter,
        ),
    )
    .apply(
        "mymap",
        Map(
            function=simple_map,
        ),
    )
    .apply(
        "myreduce",
        Reducer(
            window=reduce_window,
            aggregate_func=WordCounter,
            group_by_key=GroupByWord(),
        ),
    )
    .sink(
        "kafkasink",
        StreamSink(
            stream_name="transformed-events",
        ),
    )
)
