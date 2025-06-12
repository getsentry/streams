from typing import Generator, MutableSequence, Optional, Self, Tuple

from sentry_streams.pipeline.function_template import Accumulator, InputType
from sentry_streams.pipeline.message import Message


class BatchBuilder(
    Accumulator[Message[InputType], MutableSequence[Tuple[InputType, Optional[str]]]]
):
    """
    Takes a generic input format, and batches into a generic batch representation
    with the same input type. Returns this batch representation.

    The data type of the elements remains the same through this operation.
    """

    def __init__(self) -> None:
        self.batch: MutableSequence[Tuple[InputType, Optional[str]]] = []

    def add(self, value: Message[InputType]) -> Self:
        self.batch.append((value.payload, value.schema))

        return self

    def get_value(self) -> MutableSequence[Tuple[InputType, Optional[str]]]:
        return self.batch

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


def unbatch(
    batch: Message[MutableSequence[Tuple[InputType, Optional[str]]]],
) -> Generator[InputType, None, None]:
    """
    Takes in a generic batch representation, outputs a Generator type for iterating over
    individual elements which compose the batch.

    The data type of the elements remains the same through this operation. This operation
    may need to be followed by a Map or other transformation if a new output type is expected.
    """
    for payload, _ in batch.payload:
        yield payload
