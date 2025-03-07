import json
from typing import Self

from sentry_streams.pipeline.function_template import Accumulator, GroupBy


class EventsPipelineMapFunction:
    """
    Sample user-defined functions to
    plug into pipeline. Group together
    functions that are related (e.g.
    part of the same pipeline) into
    classes like this one.
    """

    @staticmethod
    def dumb_map(value: str) -> str:
        d = json.loads(value)
        word: str = d.get("word", "null_word")

        return "hello." + word

    @staticmethod
    def simple_map(value: str) -> tuple[str, int]:
        d = json.loads(value)
        word: str = d.get("word", "null_word")

        return (word, 1)

    @staticmethod
    def str_convert(value: tuple[str, int]) -> str:
        word, count = value

        return f"{word} {count}"


class EventsPipelineFilterFunctions:
    @staticmethod
    def simple_filter(value: str) -> bool:
        d = json.loads(value)
        return True if "name" in d else False

    @staticmethod
    def wrong_type_filter(value: str) -> str:  # type: ignore[empty-body]
        # TODO: move test functions into the tests/ folder somehow
        """
        Filter function with wrong return type, used in tests
        """
        pass


class GroupByWord(GroupBy):

    def get_group_by_key(self, payload: tuple[str, int]) -> str:
        return payload[0]


class WordCounter(Accumulator[tuple[str, int], str]):

    def __init__(self) -> None:
        self.tup = ("", 0)

    def add(self, value: tuple[str, int]) -> Self:
        self.tup = (value[0], self.tup[1] + value[1])

        return self

    def get_value(self) -> str:
        return f"{self.tup[0]} {self.tup[1]}"

    def merge(self, other: Self) -> Self:
        first = self.tup[0] + other.tup[0]
        second = self.tup[1] + other.tup[1]

        self.tup = (first, second)

        return self
