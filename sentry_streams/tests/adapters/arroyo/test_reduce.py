import time
from datetime import datetime, timedelta
from typing import Any, Sequence
from unittest import mock
from unittest.mock import call

import pytest
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, Message, Partition, Topic

from sentry_streams.adapters.arroyo.reduce import (
    TimeWindowedReduce,
    build_arroyo_windowed_reduce,
)
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.window import SlidingWindow


def make_msg(payload: Any, route: Route, offset: int) -> Message[Any]:
    return Message(
        BrokerValue(
            payload=RoutedValue(route=route, payload=payload),
            partition=Partition(Topic("test_topic"), 0),
            offset=offset,
            timestamp=datetime(2025, 1, 1, 12, 0),
        )
    )


@pytest.mark.parametrize(
    "window_size, window_slide, time_loop, acc_times, windows, window_close_times",
    [
        pytest.param(
            10.0,
            5.0,
            15,
            [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14]],
            [[0, 1], [1, 2]],
            [10, 15],
            id="Window size 10, window slide 5",
        ),
        pytest.param(5.0, 5.0, 5, [[0, 1, 2, 3, 4]], [[0]], [5], id="Tumbling Window"),
        pytest.param(
            6.0,
            2.0,
            10,
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
            [[0, 1, 2], [1, 2, 3], [2, 3, 4]],
            [6, 8, 10],
            id="Window size 6, window slide 2",
        ),
    ],
)
def test_window_initializer(
    window_size: float,
    window_slide: float,
    time_loop: int,
    acc_times: Sequence[Sequence[int]],
    windows: Sequence[Sequence[int]],
    window_close_times: Sequence[int],
) -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=window_size,
        window_slide=window_slide,
        acc=acc,
        next_step=next_step,
        route=route,
    )

    assert reduce.time_loop == time_loop
    assert reduce.acc_times == acc_times
    assert reduce.windows == windows
    assert reduce.window_close_times == window_close_times


def test_submit_and_poll() -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=6.0,
        window_slide=2.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )

    cur_time = time.time()

    reduce.submit(make_msg("test-payload", route, 0))
    reduce.submit(make_msg("test-payload", route, 1))
    reduce.submit(make_msg("test-payload", route, 2))

    with mock.patch("time.time", return_value=cur_time + 6.0):
        reduce.poll()

    next_step.submit.assert_called_once()
    next_step.poll.assert_called_once()


def test_invalid_window() -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce_window = SlidingWindow(
        window_size=timedelta(seconds=6), window_slide=timedelta(seconds=0)
    )

    with pytest.raises(ValueError):
        build_arroyo_windowed_reduce(reduce_window, acc, next_step, route)


def test_tumbling_window() -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=6.0,
        window_slide=6.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )

    cur_time = time.time()

    reduce.submit(make_msg("test-payload", route, 0))
    reduce.submit(make_msg("test-payload", route, 1))

    with mock.patch("time.time", return_value=cur_time + 4.0):
        reduce.submit(make_msg("test-payload", route, 2))

    # this poll flushes the window,
    # causing next_step.submit to be called
    with mock.patch("time.time", return_value=cur_time + 6.0):
        reduce.poll()

    # this poll does not flush any window
    # next_step.submit should still be called once
    with mock.patch("time.time", return_value=cur_time + 9.0):
        reduce.poll()

    next_step.submit.assert_called_once()
    next_step.poll.assert_has_calls([call(), call()])


# filtered payload test


@pytest.mark.parametrize(
    "window_size, window_slide",
    [
        (timedelta(seconds=4), timedelta(seconds=5)),
        (timedelta(seconds=5), timedelta(seconds=0)),
        (timedelta(seconds=5, milliseconds=500), timedelta(seconds=1, milliseconds=500)),
    ],
)
def test_bad_window_config(window_size: timedelta, window_slide: timedelta) -> None:

    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce_window = SlidingWindow(window_size=window_size, window_slide=window_slide)

    with pytest.raises(ValueError):
        build_arroyo_windowed_reduce(reduce_window, acc, next_step, route)


def test_join() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=6.0,
        window_slide=2.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )
    reduce.join()
    next_step.join.assert_called_once()


def test_close() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=6.0,
        window_slide=2.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )
    reduce.close()
    next_step.close.assert_called_once()


def test_terminate() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    acc = mock.Mock(spec=Accumulator)
    route = mock.Mock(spec=Route)

    reduce: TimeWindowedReduce[Any, Any] = TimeWindowedReduce(
        window_size=6.0,
        window_slide=2.0,
        acc=acc,
        next_step=next_step,
        route=route,
    )
    reduce.terminate()
    next_step.terminate.assert_called_once()
