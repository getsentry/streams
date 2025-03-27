import logging
import math
import sys
import time
from datetime import timedelta
from typing import Any, Callable, Generic, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, FilteredPayload, Message

from sentry_streams.adapters.arroyo.routes import RoutedValue
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


class WindowedReduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    def __init__(
        self,
        window_size: Union[int, float],
        window_slide: Union[int, float],
        acc: Callable[[], Accumulator[Any, Any]],
        next_step: ProcessingStrategy[TResult],
    ) -> None:

        window_count = math.ceil(window_size / window_slide)

        self.window_count = window_count
        self.window_size = window_size
        self.window_slide = window_slide
        self.acc = acc
        self.next_step = next_step

        # initializers depending on whether it is count-based or time-based
        if isinstance(window_size, int):
            self.count_mode = True
            self.message_count = 1

        else:
            self.count_mode = False
            self.start_time = time.time()

        # build the list of Reduces, as we know the number of windows to manage upfront
        self.windows = [self.create_reduce() for _ in range(self.window_count)]

        # we know the starting value of every window goes through a cycle
        # this represents the size of that cycle / loop
        self.window_loop = self.window_count * self.window_slide

        # since we're adding elements to a cycle effectively,
        # we know, for each window, which indices within that cycle
        # CANNOT be part of that window

        # for each window, track a 'greater' and a 'lesser' value
        # every incoming message will be compared to these to determine
        # whether or not the message can be added to the window
        self.boundaries = []
        for i in range(self.window_count):
            greater = (self.window_size + i * self.window_slide) % self.window_loop

            # this branch is because of the different meaning of window_size
            # between count and time
            # e.g. count window of 5 msgs: [msg1 ... msg5]
            # e.g. time window of 5 seconds [0s ... 5s]
            if self.count_mode:
                less = i * self.window_slide + 1
            else:
                less = i * self.window_slide

            self.boundaries.append((greater, less))

    def create_reduce(self) -> Reduce[Any, Any]:
        final_acc = ArroyoAccumulator(self.acc)

        if self.count_mode:
            return Reduce(
                int(self.window_size),
                float("inf"),
                final_acc.accumulator,
                final_acc.initial_value,
                self.next_step,
            )

        else:
            # Unfortunate
            return Reduce(
                sys.maxsize,
                self.window_size,
                final_acc.accumulator,
                final_acc.initial_value,
                self.next_step,
            )

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        # count based
        if self.count_mode:
            msg_id = self.message_count % self.window_loop

        # time based
        else:
            cur_time = time.time() - self.start_time
            msg_id = cur_time % self.window_loop

        tracked_messages = self.message_count if self.count_mode else cur_time

        # actually submit a message to the right Reduces windows
        for i in range(len(self.windows)):

            if tracked_messages > i * self.window_slide:
                if self.boundaries[i][0] < self.boundaries[i][1]:
                    if not (msg_id > self.boundaries[i][0] and msg_id < self.boundaries[i][1]):
                        logger.info(f"message index {tracked_messages}, window_id {i}")
                        self.windows[i].submit(message)

                else:
                    # this condition exists because boundary values for every window is cyclic
                    if not (msg_id > self.boundaries[i][0] or msg_id < self.boundaries[i][1]):
                        logger.info(f"message index {tracked_messages}, window_id {i}")
                        self.windows[i].submit(message)

        if self.count_mode:
            self.message_count += 1

    def poll(self) -> None:
        for i in range(self.window_count):
            self.windows[i].poll()

    def close(self) -> None:
        for i in range(self.window_count):
            self.windows[i].close()

    def terminate(self) -> None:
        for i in range(self.window_count):
            self.windows[i].terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        for i in range(self.window_count):
            self.windows[i].join(timeout)


def build_arroyo_windowed_reduce(
    streams_window: Window[MeasurementUnit],
    accumulator: Callable[[], Accumulator[Any, Any]],
    next_step: ProcessingStrategy[Union[FilteredPayload, TPayload]],
) -> ProcessingStrategy[Union[FilteredPayload, TPayload]]:

    match streams_window:
        case SlidingWindow(window_size, window_slide):
            match (window_size, window_slide):
                case (int(), int()):
                    return WindowedReduce(window_size, window_slide, accumulator, next_step)

                case (timedelta(), timedelta()):
                    return WindowedReduce(
                        window_size.total_seconds(),
                        window_slide.total_seconds(),
                        accumulator,
                        next_step,
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
                    return Reduce(
                        sys.maxsize,
                        window_size.total_seconds(),
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

                case _:
                    raise TypeError(
                        f"{type(window_size)} is not a supported MeasurementUnit type for TumblingWindow"
                    )

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")
