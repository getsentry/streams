import abc
from typing import Callable, MutableMapping, Sequence
import logging

# Import the Message class from the gRPC stub
from .flink_worker_pb2 import Message

logger = logging.getLogger(__name__)

class ProcessingSegment(abc.ABC):

    @abc.abstractmethod
    def process(self, message: Message) -> Sequence[Message]:
        """Process the input data using the chain function."""
        raise NotImplementedError

    @abc.abstractmethod
    def watermark(self, timestamp: int) -> Sequence[Message]:
        """Process a watermark event"""
        raise NotImplementedError


class Accumulator(abc.ABC):

    @abc.abstractmethod
    def add(self, value: Message) -> None:
        """
        Add values to the Accumulator. Can produce a new type which is different
        from the input type.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_value(self) -> Message:
        """
        Get the output value from the Accumulator. Can produce a new type
        which is different from the Accumulator type.
        """
        raise NotImplementedError


class WindowSegment(abc.ABC):

    def add(self, message: Message, window_time: int, window_key: str) -> None:
        """Add a message to the window"""
        raise NotImplementedError

    def trigger(self, window_time: int, window_key: str) -> Sequence[Message]:
        """Trigger the window"""
        raise NotImplementedError


class AccumulatorWindowSegment(WindowSegment):

    def __init__(self, accumulator_builder: Callable[[], Accumulator]):
        self.__accumulators: MutableMapping[str, Accumulator] = {}
        self.__builder = accumulator_builder

    def add(self, message: Message, window_time: int, window_key: str) -> None:
        key = f"{window_time}_{window_key}"
        if key not in self.__accumulators:
            self.__accumulators[key] = self.__builder()
        self.__accumulators[key].add(message)

    def trigger(self, window_time: int, window_key: str) -> Sequence[Message]:
        """Trigger the window"""
        key = f"{window_time}_{window_key}"
        if key not in self.__accumulators:
            logger.warning(f"Window {key} not found")
            return []

        val = self.__accumulators[key].get_value()
        logger.info(f"Triggering window {key} with value {val}")
        del self.__accumulators[key]
        return [val]
