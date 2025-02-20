from abc import ABC, abstractmethod
from typing import Any


class Accumulator(ABC):

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


# class GroupBy(ABC):

#     @abstractmethod
#     def get_key(payload):
#         pass
