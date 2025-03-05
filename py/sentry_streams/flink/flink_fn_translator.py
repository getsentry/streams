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
    OutputType,
)
from sentry_streams.window import (
    MeasurementUnit,
    SlidingWindow,
    TumblingWindow,
    Window,
)

FLINK_TYPE_MAP: dict[Any, Any] = {
    tuple[str, int]: (Types.TUPLE, [Types.STRING, Types.INT]),
    str: (Types.STRING, []),
    list[str]: (Types.BASIC_ARRAY, Types.STRING),
    dict[str, str]: (Types.MAP, Types.STRING, Types.STRING),
}


def is_standard_type(type: Any) -> bool:
    if type in FLINK_TYPE_MAP:
        return True

    return False


def translate_custom_type(type: Any) -> TypeInformation:
    return Types.PICKLED_BYTE_ARRAY()


def translate_to_flink_type(type: Any) -> TypeInformation:
    """
    Convert Python types to Flink's Types class.
    Recommended for use in explicitly defining the
    output_type of a Flink operator. These types
    specifically have to be instantiated.
    """
    fn: Callable[..., TypeInformation]
    fn = FLINK_TYPE_MAP[type][0]
    args = FLINK_TYPE_MAP[type][1:]

    if len(args) == 1:
        args = args[0]

        if not args:
            return fn()

        elif isinstance(args, list):
            return fn([arg() for arg in args])

        else:
            return fn(args())

    else:
        arg1, arg2 = args

        return fn(arg1(), arg2())


class FlinkAggregate(AggregateFunction, Generic[InputType, OutputType]):
    """
    Takes the Streams API's Accumulator and transforms it into Flink's
    Aggregate Function. For a streaming pipeline runing on Flink,
    this is the main mechanism for an Accumulator-based aggregation
    on a window of data. Adds incoming data to an intermediate
    aggregate and merges intermediate aggregates.
    """

    def __init__(self, acc: Callable[[], Accumulator[InputType, OutputType]]) -> None:
        self.acc = acc
        # self.backend = ...

    def create_accumulator(self) -> Accumulator[InputType, OutputType]:
        """
        Instantiates the Accumulator type for every window created.
        """
        return self.acc()

    def add(
        self, value: InputType, accumulator: Accumulator[InputType, OutputType]
    ) -> Accumulator[InputType, OutputType]:
        assert isinstance(accumulator, Accumulator)

        return accumulator.add(value)

    def get_result(self, accumulator: Accumulator[InputType, OutputType]) -> OutputType:
        assert isinstance(accumulator, Accumulator)

        accumulated = accumulator.get_value()

        # self.backend.flush(accumulated)

        return accumulated

    def merge(
        self,
        acc_a: Accumulator[InputType, OutputType],
        acc_b: Accumulator[InputType, OutputType],
    ) -> Accumulator[InputType, OutputType]:
        assert isinstance(acc_a, Accumulator)

        return acc_a.merge(acc_b)


class FlinkGroupBy(KeySelector):
    """
    Takes the Streams API's GroupBy and provides a thin
    wrapper to convert to Flink's GroupBy mechanism.
    """

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


def build_flink_window(streams_window: Window[MeasurementUnit]) -> WindowAssigner[Any, Any]:

    match streams_window:
        case SlidingWindow(window_size, window_slide):
            match window_size:
                case timedelta():
                    return SlidingEventTimeWindows.of(
                        to_flink_time(window_size), to_flink_time(window_slide)
                    )
                case int():
                    return CountSlidingWindowAssigner.of(window_size, window_slide)

                case _:
                    raise TypeError(f"{window_size} is not a supported MeasurementUnit type")

        case TumblingWindow(window_size):
            match window_size:
                case timedelta():
                    return TumblingEventTimeWindows.of(to_flink_time(window_size))

                case int():
                    return CountTumblingWindowAssigner.of(window_size)

                case _:
                    raise TypeError(f"{window_size} is not a supported MeasurementUnit type")

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")
