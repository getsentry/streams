from datetime import timedelta
from types import GeneratorType
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Mapping,
    TypeVar,
    Union,
    cast,
    get_args,
)

from pyflink.common import Time, TypeInformation, Types
from pyflink.datastream import OutputTag, ProcessFunction
from pyflink.datastream.functions import AggregateFunction, KeySelector
from pyflink.datastream.window import (
    CountSlidingWindowAssigner,
    CountTumblingWindowAssigner,
    SlidingEventTimeWindows,
    TumblingEventTimeWindows,
    WindowAssigner,
)
from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    KVAccumulator,
    KVAggregationBackend,
    OutputType,
)
from sentry_streams.pipeline.window import (
    MeasurementUnit,
    SlidingWindow,
    TumblingWindow,
    Window,
)

RoutingFuncReturnType = TypeVar("RoutingFuncReturnType")

FLINK_TYPE_MAP: dict[Any, Any] = {
    tuple[str, int]: (Types.TUPLE, [Types.STRING, Types.INT]),
    str: (Types.STRING, []),
    list[str]: (Types.BASIC_ARRAY, Types.STRING),
    dict[str, str]: (Types.MAP, Types.STRING, Types.STRING),
}


def is_standard_type(type: Any) -> bool:
    return type in FLINK_TYPE_MAP


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


def translate_custom_type(type: Any) -> TypeInformation:
    """
    For Python types which use Pickle for serialization/deserialization
    and do not have a matching standard Flink Type. e.g. custom-defined
    classes. Also handles Generator type.
    """

    assert not is_standard_type(type)

    if isinstance(type, GeneratorType):
        return translate_to_flink_type(get_args(type)[0])

    else:
        return Types.PICKLED_BYTE_ARRAY()


class FlinkAggregate(AggregateFunction, Generic[InputType, OutputType]):
    """
    Takes the Streams API's Accumulator and transforms it into Flink's
    Aggregate Function. For a streaming pipeline runing on Flink,
    this is the main mechanism for an Accumulator-based aggregation
    on a window of data. Adds incoming data to an intermediate
    aggregate and merges intermediate aggregates.
    """

    def __init__(
        self,
        acc: Callable[[], Accumulator[InputType, OutputType]],
        backend: Union[AggregationBackend[OutputType], None],
    ) -> None:
        self.acc = acc
        self.backend = backend

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

        if self.backend:
            # TODO: Consider moving this check
            if isinstance(accumulator, KVAccumulator):
                cast(KVAggregationBackend, self.backend)
                self.backend.flush_aggregate(accumulated)

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
            match (window_size, window_slide):
                case (timedelta(), timedelta()):
                    return SlidingEventTimeWindows.of(
                        to_flink_time(window_size), to_flink_time(window_slide)
                    )
                case (int(), int()):
                    return CountSlidingWindowAssigner.of(window_size, window_slide)

                case _:
                    raise TypeError(
                        f"({type(window_size)}, {type(window_slide)}) is not a supported MeasurementUnit type combination for SlidingWindow"
                    )

        case TumblingWindow(window_size):
            match window_size:
                case timedelta():
                    return TumblingEventTimeWindows.of(to_flink_time(window_size))

                case int():
                    return CountTumblingWindowAssigner.of(window_size)

                case _:
                    raise TypeError(
                        f"{type(window_size)} is not a supported MeasurementUnit type for TumblingWindow"
                    )

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")


def get_router_message_type(routing_func: Callable[..., RoutingFuncReturnType]) -> type:
    """
    We have to derive the type of the messages being passed through a Router
    by looking at the type of the routing function's input parameter.

    If the routing function is a lambda, it doen't have `__annotations__`,
    so we assume the type of the message can be anything.
    """
    routing_func_attr = routing_func.__annotations__
    routing_func_params = [key for key in routing_func_attr if key != "return"]
    if routing_func_params:
        assert (
            len(routing_func_params) == 1
        ), f"Routing functions should only have a single parameter, got multiple: {routing_func_params}"
        input_param_name = routing_func_params[0]
        message_type: type = routing_func_attr[input_param_name]
    else:
        message_type = object
    return message_type


class FlinkRoutingFunction(ProcessFunction):
    def __init__(
        self,
        routing_func: Callable[..., RoutingFuncReturnType],
        output_tags: Mapping[RoutingFuncReturnType, OutputTag],
    ) -> None:
        super().__init__()
        self.routing_func = routing_func
        self.output_tags = output_tags

    def process_element(
        self, value: Any, ctx: ProcessFunction.Context
    ) -> Generator[tuple[OutputTag, Any], None, None]:
        output_stream = self.routing_func(value)
        output_label = self.output_tags[output_stream]
        yield output_label, value
