# TODO: Build a generic BatchBuilder provided by the platform. Provides a JSON string output.
from typing import Self

from sentry_streams.pipeline.function_template import Accumulator


class BatchBuilder(Accumulator[str, str]):
    """
    Takes str input and accumulates them into a batch array.
    Joins back into a string to produce onto a Kafka topic.
    """

    def __init__(self) -> None:
        self.batch: list[str] = []

    def add(self, value: str) -> Self:
        self.batch.append(value)

        return self

    def get_value(self) -> str:
        return " ".join(self.batch)

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self
