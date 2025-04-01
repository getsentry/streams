import time
from datetime import datetime, timedelta
from typing import Any, Sequence
from unittest import mock

import pytest
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, Message, Partition, Topic

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.translator import (
    WindowedReduce,
    build_arroyo_windowed_reduce,
)
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.window import SlidingWindow


def make_msg(payload: Any, route: Route, offset: int, ts: int) -> Message[Any]:
    return Message(
        BrokerValue(
            payload=RoutedValue(route=route, payload=payload),
            partition=Partition(Topic("test_topic"), 0),
            offset=offset,
            timestamp=datetime(2025, 1, 1, 12, 0, ts),
        )
    )


@pytest.mark.parametrize(
    "window_size, window_slide, largest_val, acc_times, windows, window_close_times",
    [
        (
            10.0,
            5.0,
            15,
            [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14]],
            [[0, 1], [1, 2]],
            [10, 15],
        ),
        (5.0, 5.0, 5, [[0, 1, 2, 3, 4]], [[0]], [5]),
        (
            6.0,
            2.0,
            10,
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
            [[0, 1, 2], [1, 2, 3], [2, 3, 4]],
            [6, 8, 10],
        ),
    ],
)
def test_window_initializer(
    window_size: float,
    window_slide: float,
    largest_val: int,
    acc_times: Sequence[Sequence[int]],
    windows: Sequence[Sequence[int]],
    window_close_times: Sequence[int],
) -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: WindowedReduce[Any, Any] = WindowedReduce(
        window_size=window_size,
        window_slide=window_slide,
        acc=acc,
        next_step=next_step,
        route=route,
    )

    assert reduce.largest_val == largest_val
    assert reduce.acc_times == acc_times
    assert reduce.windows == windows
    assert reduce.window_close_times == window_close_times


def test_submit() -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: WindowedReduce[Any, Any] = WindowedReduce(
        window_size=6.0,
        window_slide=2.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )

    cur_time = time.time()

    reduce.submit(make_msg("test-payload", route, 0, 2))
    reduce.submit(make_msg("test-payload", route, 1, 4))
    reduce.submit(make_msg("test-payload", route, 2, 6))

    with mock.patch("time.time", return_value=cur_time + 6.0):
        reduce.poll()

    next_step.submit.assert_called_once()


def test_invalid_window() -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce_window = SlidingWindow(
        window_size=timedelta(seconds=6), window_slide=timedelta(seconds=0)
    )

    with pytest.raises(ValueError):
        build_arroyo_windowed_reduce(reduce_window, acc, next_step, route)


# tumbling window test

# filtered payload test

# second-precision only test
