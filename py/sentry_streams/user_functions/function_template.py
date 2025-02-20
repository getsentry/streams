from abc import ABC, abstractmethod
from typing import Any


class Accumulator(ABC):
    """
    The standard Accumulator template.
    Define these functions to build a custom
    Accumulator for aggregation.
    """

    @abstractmethod
    def create(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def add(self, acc: Any, value: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def get_output(self, acc: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def merge(self, acc1: Any, acc2: Any) -> Any:
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
