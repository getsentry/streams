from datetime import timedelta
from typing import Any

from pyflink.common import Time, Types
from pyflink.datastream.functions import AggregateFunction, KeySelector
from pyflink.datastream.window import (
    CountSlidingWindowAssigner,
    CountTrigger,
    CountTumblingWindowAssigner,
)
from pyflink.datastream.window import EventTimeTrigger as FlinkEventTimeTrigger
from pyflink.datastream.window import (
    SlidingEventTimeWindows,
)
from pyflink.datastream.window import Trigger as FlinkTrigger
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    WindowAssigner,
)

from sentry_streams.user_functions.function_template import Accumulator, GroupBy
from sentry_streams.window import (
    CountingTrigger,
    EventTimeTrigger,
    SlidingCountWindow,
    SlidingEventTimeWindow,
    Trigger,
    TumblingCountWindow,
    TumblingEventTimeWindow,
    Window,
)

FLINK_TYPE_MAP = {tuple[str, int]: Types.TUPLE([Types.STRING(), Types.INT()]), str: Types.STRING()}


class FlinkAggregate(AggregateFunction):

    def __init__(self, acc: Accumulator) -> None:
        self.acc = acc

    def create_accumulator(self) -> Any:
        return self.acc.create()

    def add(self, value: Any, accumulator: Any) -> Any:
        return self.acc.add(accumulator, value)

    def get_result(self, accumulator: Any) -> Any:
        return self.acc.get_output(accumulator)

    def merge(self, acc_a: Any, acc_b: Any) -> Any:
        return self.acc.merge(acc_a, acc_b)


class FlinkGroupBy(KeySelector):

    def __init__(self, group_by: GroupBy) -> None:
        self.group_by = group_by

    def get_key(self, value: Any) -> Any:
        return self.group_by.get_group_by_key(value)


def to_flink_time(timestamp: timedelta) -> Time:
    """
    Convert a timedelta object to a Time type representation in milliseconds.
    """

    total_milliseconds: int = int(timestamp.total_seconds() * 1000)

    flink_time: Time = Time.milliseconds(total_milliseconds)
    return flink_time


class FlinkWindows:

    def __init__(self, window: Window) -> None:
        self.window = window

    def build_window(self) -> WindowAssigner[Any, Any]:

        window: Window = self.window

        match window:
            case SlidingEventTimeWindow(_, window_size, window_slide):
                return SlidingEventTimeWindows.of(
                    to_flink_time(window_size), to_flink_time(window_slide)
                )

            case SlidingCountWindow(_, window_size, window_slide):
                return CountSlidingWindowAssigner.of(window_size, window_slide)

            case TumblingEventTimeWindow(_, window_size):
                return TumblingEventTimeWindows.of(to_flink_time(window_size))

            case TumblingCountWindow(_, window_size):
                return CountTumblingWindowAssigner.of(window_size)

            case _:
                raise TypeError(f"{window} is not a supported Window type")

    def get_trigger(self) -> FlinkTrigger[Any, Any]:

        trigger: Trigger = self.window.trigger

        match trigger:

            case CountingTrigger(count):
                return CountTrigger(count)

            case EventTimeTrigger():
                return FlinkEventTimeTrigger()

            case _:
                raise TypeError(f"{trigger} is not a supported Trigger type")
