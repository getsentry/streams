from typing import Generator, Self

from sentry_streams.pipeline.function_template import Accumulator, InputType


class BatchBuilder(Accumulator[InputType, list[InputType]]):
    """
    Takes str input and accumulates them into a batch array.
    Joins back into a string to produce onto a Kafka topic.
    """

    def __init__(self) -> None:
        self.batch: list[InputType] = []

    def add(self, value: InputType) -> Self:
        self.batch.append(value)

        return self

    def get_value(self) -> list[InputType]:
        return self.batch

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


def unbatch(batch: list[InputType]) -> Generator[InputType, None, None]:
    for message in batch:
        yield message
