import logging
import math
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

    def __init__(
        self,
        acc: Callable[[], Accumulator[Any, Any]],
    ) -> None:
        self.acc = acc

    def initial_value(self) -> Any:
        # instantaiate the underlying accumulator every time
        self.instance = self.acc()

        # get the fresh initial value each time
        return self.instance.get_value()

    # types still tbd
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
    as well as all the Accumulator functions.
    """

    def __init__(self, acc: Callable[[], Accumulator[Any, Any]]):
        self.acc = acc()
        self.offsets: MutableMapping[Partition, int] = {}
        self.timestamp = time.time()

    def add(self, value: Any) -> Self:

        payload = value.payload.payload
        offsets: MutableMapping[Partition, int] = value.committable

        self.acc.add(payload)

        self.offsets.update(offsets)

        return self

    def get_value(self) -> Any:

        return self.acc.get_value()

    def merge(self, other: Self) -> Self:

        for partition in other.offsets:
            if partition in self.offsets:
                self.offsets[partition] = max(self.offsets[partition], other.offsets[partition])

            else:
                self.offsets[partition] = other.offsets[partition]

        self.acc.merge(other.acc)

        return self

    def get_offsets(self) -> MutableMapping[Partition, int]:

        return self.offsets

    def clear(self) -> None:

        assert hasattr(self.acc, "clear")
        self.acc.clear()
        self.offsets = {}

        return


class WindowedReduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    def __init__(
        self,
        window_size: float,
        window_slide: float,
        acc: Callable[[], Accumulator[Any, Any]],
        next_step: ProcessingStrategy[TResult],
        route: Route,
    ) -> None:

        window_count = math.ceil(window_size / window_slide)

        self.window_count = window_count
        self.window_size = int(window_size)
        self.window_slide = int(window_slide)

        self.next_step = next_step
        self.start_time = time.time()
        self.route = route

        # build a list of Accumulators
        self.largest_val: int = int(2 * self.window_size - self.window_slide)
        num_accs: int = int(self.largest_val // self.window_slide)
        self.accs = [KafkaAccumulator(acc) for _ in range(num_accs)]

        self.acc_times = [
            list(range(i, i + self.window_slide))
            for i in range(0, self.largest_val, self.window_slide)
        ]

        accs_per_window = self.window_size // self.window_slide

        # list of acc IDs per window ID
        self.windows = [
            list(range(i, i + accs_per_window)) for i in range(num_accs - accs_per_window + 1)
        ]

        # the next times at which each window will close
        self.window_close_times = [
            float(self.window_size + self.window_slide * i) for i in range(self.window_count)
        ]
        self.__closed = False

    def __merge_and_flush(self, window_id: int) -> None:
        accs_to_merge: list[int] = self.windows[window_id]
        first_acc_id = accs_to_merge[0]

        merged_window: KafkaAccumulator = self.accs[first_acc_id]
        for i in accs_to_merge[1:]:
            acc = self.accs[i]
            merged_window.merge(acc)

        payload = merged_window.get_value()

        if payload:
            result = RoutedValue(self.route, payload)
            self.next_step.submit(
                Message(Value(cast(TResult, result), merged_window.get_offsets()))
            )

        # clear only the merged window
        merged_window.clear()
        self.accs[first_acc_id] = merged_window

    def __maybe_flush(self, cur_time: float) -> None:

        for i in range(len(self.windows)):
            window = self.windows[i]

            if cur_time >= self.window_close_times[i]:
                # FLUSH any window where this id doesn't show up
                self.__merge_and_flush(i)

                # only shift a window if it was flushed
                window = [(t + len(window)) % len(self.accs) for t in window]
                self.windows[i] = window

                self.window_close_times[i] += float(self.window_size)

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:

        assert not self.__closed

        value = message.payload
        if isinstance(value, FilteredPayload):
            self.next_step.submit(cast(Message[TResult], message))
            return

        assert isinstance(value, RoutedValue)
        if value.route != self.route:
            self.next_step.submit(cast(Message[TResult], message))
            return

        cur_time = time.time() - self.start_time
        acc_id = int((cur_time % self.largest_val) // self.window_slide)

        self.__maybe_flush(cur_time)

        self.accs[acc_id].add(message.value)

    def poll(self) -> None:
        assert not self.__closed

        cur_time = time.time() - self.start_time
        # acc_id = (cur_time % self.largest_val) // self.window_slide

        self.__maybe_flush(cur_time)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.next_step.close()
        self.next_step.join()


def build_arroyo_windowed_reduce(
    streams_window: Window[MeasurementUnit],
    accumulator: Callable[[], Accumulator[Any, Any]],
    next_step: ProcessingStrategy[Union[FilteredPayload, TPayload]],
    route: Route,
) -> ProcessingStrategy[Union[FilteredPayload, TPayload]]:

    match streams_window:
        case SlidingWindow(window_size, window_slide):
            match (window_size, window_slide):
                case (timedelta(), timedelta()):
                    return WindowedReduce(
                        window_size.total_seconds(),
                        window_slide.total_seconds(),
                        accumulator,
                        next_step,
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
                        next_step,
                    )

                case timedelta():
                    return WindowedReduce(
                        window_size.total_seconds(),
                        window_size.total_seconds(),
                        accumulator,
                        next_step,
                        route,
                    )

                case _:
                    raise TypeError(
                        f"{type(window_size)} is not a supported MeasurementUnit type for TumblingWindow"
                    )

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")
