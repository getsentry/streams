from abc import ABC, abstractmethod
from typing import Any, Generic, Self, TypeVar

InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")


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


class KVAggregationBackend(AggregationBackend[dict[Any, Any]]):
    """
    A storage backend that is meant to store windowed key-value pair aggregates.
    This class supports basic in-memory mappings. Extend this class
    for different storage configurations for storing K-Vs.
    """

    def __init__(self) -> None:
        self.map: dict[Any, Any] = {}

    def flush_aggregate(self, aggregate: dict[Any, Any]) -> None:
        """
        Flush a windowed aggregate to storage. Takes in the output from
        the Accumulator.
        """

        for k, v in aggregate.items():
            self.map[k] = v


class Accumulator(ABC, Generic[InputType, OutputType]):
    """
    The template for building Accumulators, which use windowed
    aggregations. Extend this to build a custom Accumulator, defining the
    data schema of the input type and the output type (the aggregate).

    Specifically exposes a merge() API to implement. See examples/
    for samples on how it should be used.
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
