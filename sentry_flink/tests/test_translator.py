from datetime import timedelta

import pytest
from pyflink.common import Time
from sentry_streams.pipeline.window import (
    SlidingWindow,
    TumblingWindow,
)

from sentry_flink.flink.flink_translator import (
    build_flink_window,
    to_flink_time,
)


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        (timedelta(seconds=30), Time.milliseconds(30000)),
        (timedelta(minutes=5), Time.milliseconds(300000)),
        (timedelta(minutes=5, seconds=40), Time.milliseconds(340000)),
    ],
)
def test_time_conversion(timestamp, expected):

    flink_ts = to_flink_time(timestamp)

    assert flink_ts == expected


# class TestWindow(Window[MeasurementUnit]):
#     pass


@pytest.mark.parametrize(
    "window, expected",
    [
        (
            TumblingWindow(window_size=timedelta(seconds=45)),
            "TumblingEventTimeWindows(45000, 0)",
        ),
        (SlidingWindow(window_size=3, window_slide=4), "CountSlidingWindowAssigner(3, 4)"),
    ],
)
def test_build_windows(window, expected):

    flink_window = str(build_flink_window(window))

    assert flink_window == expected


@pytest.mark.parametrize(
    "window, expected",
    [
        (
            SlidingWindow(window_size=timedelta(seconds=30), window_slide=2),
            pytest.raises(TypeError),
        )
    ],
)
def test_bad_window(window, expected):

    with expected:
        build_flink_window(window)
