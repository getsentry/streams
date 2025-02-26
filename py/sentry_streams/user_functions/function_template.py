from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
IntermediateType = TypeVar("IntermediateType")


class Accumulator(ABC, Generic[InputType, IntermediateType, OutputType]):
    """
    The standard Accumulator template.
    Define these functions to build a custom
    Accumulator for aggregation.
    """

    @abstractmethod
    def create(self) -> IntermediateType:
        raise NotImplementedError

    @abstractmethod
    def add(self, acc: IntermediateType, value: InputType) -> IntermediateType:
        raise NotImplementedError

    @abstractmethod
    def get_output(self, acc: IntermediateType) -> OutputType:
        raise NotImplementedError

    @abstractmethod
    def merge(self, acc1: IntermediateType, acc2: IntermediateType) -> IntermediateType:
        raise NotImplementedError


class GroupBy(ABC):
    """
    The standard GroupBy / keying template.
    Extend this to create your own custom
    GroupBy.
    """

    @abstractmethod
    def get_group_by_key(self, payload: Any) -> Any:
        raise NotImplementedError
