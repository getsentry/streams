from typing import MutableMapping, Self

from sentry_streams.user_functions.function_template import (
    Accumulator,
    AggregationBackend,
)

WordCountTuple = tuple[str, int]
WordCountStr = str


class WordCounterAggregationBackend(AggregationBackend[WordCountStr]):
    """
    Simple hash map backend for the WordCounter sample pipeline.
    """

    def __init__(self) -> None:
        self.storage_map: MutableMapping[str, int] = {}

    def flush_aggregate(self, aggregate: WordCountStr) -> None:

        k, v = aggregate.split(" ")

        self.storage_map[k] = int(v)


class WordCounter(Accumulator[WordCountTuple, WordCountStr]):

    def __init__(self) -> None:
        self.tup = ("", 0)

    def add(self, value: WordCountTuple) -> Self:
        self.tup = (self.tup[0] + value[0], self.tup[1] + value[1])

        return self

    def get_output(self) -> WordCountStr:
        return f"{self.tup[0]} {self.tup[1]}"

    def merge(self, other: Self) -> Self:
        first = self.tup[0] + other.tup[0]
        second = self.tup[1] + other.tup[1]

        self.tup = (first, second)

        return self
