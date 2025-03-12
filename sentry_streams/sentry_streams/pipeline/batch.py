from typing import Generator, MutableSequence, Self

from sentry_streams.pipeline.function_template import Accumulator, InputType


class BatchBuilder(Accumulator[InputType, MutableSequence[InputType]]):
    """
    Takes a generic input format, and batches into a generic batch representation
    with the same input type. Returns this batch representation.

    The data type of the elements remains the same through this operation.
    """

    def __init__(self) -> None:
        self.batch: MutableSequence[InputType] = []

    def add(self, value: InputType) -> Self:
        self.batch.append(value)

        return self

    def get_value(self) -> MutableSequence[InputType]:
        return self.batch

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


def unbatch(batch: MutableSequence[InputType]) -> Generator[InputType, None, None]:
    """
    Takes in a generic batch representation, outputs a Generator type for iterating over
    individual elements which compose the batch.

    The data type of the elements remains the same through this operation. This operation
    may need to be followed by a Map or other transformation if a new output type is expected.
    """
    for message in batch:
        yield message
