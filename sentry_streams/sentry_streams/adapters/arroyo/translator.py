import logging
from typing import Any, Callable, TypeVar, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload

from sentry_streams.adapters.arroyo.custom_strategies import WindowedReduce
from sentry_streams.adapters.arroyo.routes import RoutedValue
from sentry_streams.pipeline.function_template import Accumulator, InputType, OutputType
from sentry_streams.pipeline.window import (
    MeasurementUnit,
    SlidingWindow,
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

    def accumulator(self, result: Any, value: Any) -> Any:
        self.instance.add(value.payload.payload["test"])

        routed = RoutedValue(
            route=value.payload.route,
            payload=self.instance.get_value(),
        )

        return routed


def build_arroyo_windowed_reduce(
    streams_window: Window[MeasurementUnit],
    accumulator: Callable[[], Accumulator[InputType, OutputType]],
    next_step: ProcessingStrategy[OutputType],
) -> ProcessingStrategy[Union[FilteredPayload, TPayload]]:

    arroyo_acc = ArroyoAccumulator
    # arroyo_init: Callable[[], OutputType] = arroyo_acc.initial_value
    # arroyo_acc_fn: Callable[[OutputType, InputType], OutputType] = arroyo_acc.accumulator

    match streams_window:
        case SlidingWindow(window_size, window_slide):
            match (window_size, window_slide):
                case (int(), int()):
                    return WindowedReduce(
                        window_size, window_slide, arroyo_acc, accumulator, next_step
                    )

                case _:
                    raise TypeError(
                        f"({type(window_size)}, {type(window_slide)}) is not a supported MeasurementUnit type combination for SlidingWindow"
                    )

        # case TumblingWindow(window_size):
        #     match window_size:
        #         case int():
        #             logger.info("Correct windowing")
        #             return Reduce(window_size, float('inf'), arroyo_acc_fn, arroyo_init, next_step)

        #         case timedelta():
        #             return Reduce(window_size, window_size, arroyo_acc_fn, arroyo_init, next_step)

        #         case _:
        #             raise TypeError(
        #                 f"{type(window_size)} is not a supported MeasurementUnit type for TumblingWindow"
        #             )

        case _:
            raise TypeError(f"{streams_window} is not a supported Window type")
