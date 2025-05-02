import logging
import time
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Generic,
    MutableMapping,
    Optional,
    Self,
    TypeVar,
    Union,
    cast,
)

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, FilteredPayload, Message, Partition, Value

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.window import (
    MeasurementUnit,
    SlidingWindow,
    TumblingWindow,
    Window,
)

TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")

logger = logging.getLogger(__name__)


class ArroyoAccumulator:
    """
    A simple wrapper around Streams API's Accumulator which
    exposes the methods that Arroyo's accumulator mechanism (within Reduce)
    expects.

    If count-based Tumbling windows become absorbed by WindowedReduce,
    this will no longer be necessary.
    """

    def __init__(
        self,
        acc: Callable[[], Accumulator[Any, Any]],
    ) -> None:
        self.acc = acc

    def initial_value(self) -> Any:
        # instantaiate the underlying accumulator every time
        self.instance = self.acc()

        # get the fresh initial value
        return self.instance.get_value()

    def accumulator(self, result: Any, value: BaseValue[RoutedValue]) -> RoutedValue:
        self.instance.add(value.payload.payload)

        routed = RoutedValue(
            route=value.payload.route,
            payload=self.instance.get_value(),
        )

        return routed


class KafkaAccumulator:
    """
    Does internal bookkeeping of offsets and timestamps,
    as well as shares all the functionality of an Accumulator.

    It simply calls the underlying Accumulator.
    """

    def __init__(self, acc: Callable[[], Accumulator[Any, Any]]):
        self.acc = acc()
        self.offsets: MutableMapping[Partition, int] = {}
        self.timestamp = time.time()

    def add(self, value: Any) -> Self:

        payload = value.payload.payload
        offsets: MutableMapping[Partition, int] = value.committable
        self.acc.add(payload)

        for partition in offsets:
            if partition in self.offsets:
                self.offsets[partition] = max(offsets[partition], self.offsets[partition])

            else:
                self.offsets[partition] = offsets[partition]

        return self

    def get_value(self) -> Any:

        return self.acc.get_value()

    def merge(self, other: Self) -> Self:

        self.acc.merge(other.acc)

        return self

    def get_offsets(self) -> MutableMapping[Partition, int]:
        # return the offsets of the Accumulator we are merging against
        # when we call this method, we know this Accumulator instance
        # is ready to be cleared (not part of any further windows)
        return self.offsets


class TimeWindowedReduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    """
    Supports both sliding and tumbling windows. For now, it only supports time durations
    that are up to the second precision. For example, 5 min 30 sec is supported, but not
    5 sec 500 milliseconds.

    Currently moves and populates windows based on processing time.
    """

    def __init__(
        self,
        window_size: float,
        window_slide: float,
        acc: Callable[[], Accumulator[Any, Any]],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
        route: Route,
    ) -> None:

        self.window_count = int(window_size / window_slide)
        self.window_size = int(window_size)
        self.window_slide = int(window_slide)

        self.msg_wrap_step = next_step
        self.start_time = time.time()
        self.route = route

        self.acc = acc

        # Every sliding window has a time/duration that we loop around
        # e.g window_size = 6, window_slide = 2, time loop = 10
        # Accumulators: [0s, 1s] [2s, 3s] [4s, 5s] [6s, 7s] [8s, 9s]
        self.time_loop = int(2 * self.window_size - self.window_slide)
        num_accs = int(self.time_loop // self.window_slide)

        # Maintain a list of Accumulators
        self.accs = [KafkaAccumulator(acc) for _ in range(num_accs)]

        self.acc_times = [
            list(range(i, i + self.window_slide))
            for i in range(0, self.time_loop, self.window_slide)
        ]

        accs_per_window = self.window_size // self.window_slide

        # Each window id (represented by index) maps to a set of acc ids
        self.windows = [
            list(range(i, i + accs_per_window)) for i in range(num_accs - accs_per_window + 1)
        ]

        # Tracks the next times at which each window will close
        self.window_close_times = [
            float(self.window_size + self.window_slide * i) for i in range(self.window_count)
        ]

    def __merge_and_flush(self, window_id: int) -> None:
        accs_to_merge: list[int] = self.windows[window_id]
        first_acc_id = accs_to_merge[0]

        merged_window: KafkaAccumulator = self.accs[first_acc_id]
        for i in accs_to_merge[1:]:
            acc = self.accs[i]
            merged_window.merge(acc)

        payload = merged_window.get_value()

        # If there is a gap in the data, it is possible to have empty flushes
        if payload:
            self.msg_wrap_step.submit(
                Message(Value(cast(TResult, payload), merged_window.get_offsets()))
            )

        # Refresh only the accumulator that was the first
        # accumulator in the flushed window
        self.accs[first_acc_id] = KafkaAccumulator(self.acc)

    def __maybe_flush(self, cur_time: float) -> None:

        for i in range(len(self.windows)):
            window = self.windows[i]

            if cur_time >= self.window_close_times[i]:
                self.__merge_and_flush(i)

                # Only shift a window if it was flushed
                window = [(t + len(window)) % len(self.accs) for t in window]
                self.windows[i] = window

                self.window_close_times[i] += float(self.window_size)

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        value = message.payload
        if isinstance(value, FilteredPayload):
            self.msg_wrap_step.submit(cast(Message[Union[FilteredPayload, TResult]], message))
            return

        assert isinstance(value, RoutedValue)
        if value.route != self.route:
            self.msg_wrap_step.submit(cast(Message[Union[FilteredPayload, TResult]], message))
            return

        cur_time = time.time() - self.start_time
        acc_id = int((cur_time % self.time_loop) // self.window_slide)

        self.__maybe_flush(cur_time)

        self.accs[acc_id].add(message.value)

    def poll(self) -> None:
        cur_time = time.time() - self.start_time
        self.__maybe_flush(cur_time)

        self.msg_wrap_step.poll()

    def close(self) -> None:
        self.msg_wrap_step.close()

    def terminate(self) -> None:
        self.msg_wrap_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.msg_wrap_step.close()
        self.msg_wrap_step.join()


def build_arroyo_windowed_reduce(
    streams_window: Window[MeasurementUnit],
    accumulator: Callable[[], Accumulator[Any, Any]],
    msg_wrapper: ProcessingStrategy[Union[FilteredPayload, TPayload]],
    route: Route,
) -> ProcessingStrategy[Union[FilteredPayload, TPayload]]:

    match streams_window:
        case SlidingWindow(window_size, window_slide):
            match (window_size, window_slide):
                case (timedelta(), timedelta()):

                    size = window_size.total_seconds()
                    slide = window_slide.total_seconds()

                    # TODO: Move this validation to where a SlidingWindow gets created
                    if slide == 0.0 or slide > size:
                        raise ValueError(
                            f"Window slide {slide} cannot be 0 or larger than window size {size}"
                        )

                    if not (size).is_integer() or not (slide).is_integer():
                        raise ValueError(
                            "Currently only second precision is supported for window size and window slide"
                        )

                    return TimeWindowedReduce(
                        size,
                        slide,
                        accumulator,
                        msg_wrapper,
                        route,
                    )

                case _:
                    raise TypeError(
                        f"({type(window_size)}, {type(window_slide)}) is not a supported MeasurementUnit type combination for SlidingWindow"
                    )

        case TumblingWindow(window_size):

            arroyo_acc = ArroyoAccumulator(accumulator)

            match window_size:
                case int():
                    return Reduce(
                        window_size,
                        float("inf"),
                        cast(
                            Callable[
                                [FilteredPayload | TPayload, BaseValue[TPayload]],
                                FilteredPayload | TPayload,
                            ],
                            arroyo_acc.accumulator,
                        ),
                        arroyo_acc.initial_value,
                        msg_wrapper,
                    )

                case timedelta():
                    return TimeWindowedReduce(
                        window_size.total_seconds(),
                        window_size.total_seconds(),
                        accumulator,
                        msg_wrapper,
                        route,
                    )

                case _:
                    raise TypeError(
                        f"{type(window_size)} is not a supported MeasurementUnit type for TumblingWindow"
                    )

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")
