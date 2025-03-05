from abc import ABC, abstractmethod
from typing import Any, Generic, Self, TypeVar

InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
IntermediateType = TypeVar("IntermediateType")


class AggregationBackend(ABC, Generic[OutputType]):
    """
    A storage backend that is meant to store windowed aggregates. Configurable
    to the type of storage.
    """

    @abstractmethod
    def flush_aggregate(self, aggregate: OutputType) -> None:
        """
        Flush a windowed aggregate to storage. Takes in the output from
        the Accumulator.
        """


class Accumulator(ABC, Generic[InputType, OutputType]):
    """
    The standard Accumulator template.
    Define these functions to build a custom
    Accumulator for aggregation.
    """

    @abstractmethod
    def add(self, value: InputType) -> Self:
        """
        Add values to the Accumulator. Can produce a new type which is different
        from the input type.
        """
        raise NotImplementedError

    @abstractmethod
    def get_value(self) -> OutputType:
        """
        Get the output value from the Accumulator. Can produce a new type
        which is different from the Accumulator type.
        """
        raise NotImplementedError

    @abstractmethod
    def merge(self, other: Self) -> Self:
        """
        Merge 2 different Accumulators. Must produce the same type as Accumulator.
        Allows for merging of different intermediate values during
        distributed aggregations.
        """
        raise NotImplementedError


class KVAccumulator(Accumulator[InputType, dict[Any, Any]]):
    """
    A KVAccumulator explicitly outputs a KV mapping.
    """

    @abstractmethod
    def get_value(self) -> dict[Any, Any]:
        """
        Get the output value from the Accumulator. Can produce a new type
        which is different from the Accumulator type.
        """
        raise NotImplementedError


class GroupBy(ABC):
    """
    The standard GroupBy / keying template.
    Extend this to create your own custom
    GroupBy.
    """

    @abstractmethod
    # TODO: The payload type here will be the output
    # type from the prior Step.
    # TODO: Represent the group by key type as a Generic
    # which will be passed through to Accumulator.
    def get_group_by_key(self, payload: Any) -> Any:
        raise NotImplementedError
