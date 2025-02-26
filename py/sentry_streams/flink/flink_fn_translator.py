from datetime import timedelta
from typing import Any, Callable, Generic

from pyflink.common import Time, TypeInformation, Types
from pyflink.datastream.functions import AggregateFunction, KeySelector
from pyflink.datastream.window import (
    CountSlidingWindowAssigner,
    CountTumblingWindowAssigner,
    SlidingEventTimeWindows,
    TumblingEventTimeWindows,
    WindowAssigner,
)

from sentry_streams.user_functions.function_template import (
    Accumulator,
    GroupBy,
    InputType,
    IntermediateType,
    OutputType,
)
from sentry_streams.window import (
    MeasurementUnit,
    SlidingCountWindow,
    SlidingEventTimeWindow,
    TumblingCountWindow,
    TumblingEventTimeWindow,
    Window,
)

FLINK_TYPE_MAP: dict[Any, Any] = {
    tuple[str, int]: (Types.TUPLE, [Types.STRING, Types.INT]),
    str: (Types.STRING, []),
}


def convert_to_flink_type(type: Any) -> TypeInformation:

    fn: Callable[..., TypeInformation]
    fn, args = FLINK_TYPE_MAP[type]
    flink_type = fn([arg() for arg in args]) if args else fn()

    return flink_type


class FlinkAggregate(AggregateFunction, Generic[InputType, IntermediateType, OutputType]):

    def __init__(self, acc: Accumulator[InputType, IntermediateType, OutputType]) -> None:
        self.acc = acc

    def create_accumulator(self) -> IntermediateType:
        return self.acc.create()

    def add(self, value: InputType, accumulator: IntermediateType) -> IntermediateType:
        return self.acc.add(accumulator, value)

    def get_result(self, accumulator: IntermediateType) -> OutputType:
        return self.acc.get_output(accumulator)

    def merge(
        self,
        acc_a: IntermediateType,
        acc_b: IntermediateType,
    ) -> IntermediateType:
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


class FlinkWindows(Generic[MeasurementUnit]):

    def __init__(self, window: Window[MeasurementUnit]) -> None:
        self.window: Window[MeasurementUnit] = window

    def build_window(self) -> WindowAssigner[Any, Any]:

        window: Window[MeasurementUnit] = self.window

        match window:
            case SlidingEventTimeWindow(window_size, window_slide):
                return SlidingEventTimeWindows.of(
                    to_flink_time(window_size), to_flink_time(window_slide)
                )

            case SlidingCountWindow(window_size, window_slide):
                return CountSlidingWindowAssigner.of(window_size, window_slide)

            case TumblingEventTimeWindow(window_size):
                return TumblingEventTimeWindows.of(to_flink_time(window_size))

            case TumblingCountWindow(window_size):
                return CountTumblingWindowAssigner.of(window_size)

            case _:
                raise TypeError(f"{window} is not a supported Window type")
