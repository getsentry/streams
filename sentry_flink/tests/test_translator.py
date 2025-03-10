from datetime import timedelta
from typing import Self

import pytest
from pyflink.common import Time
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.window import (
    MeasurementUnit,
    SlidingWindow,
    TumblingWindow,
    Window,
)

from sentry_flink.flink.flink_translator import (
    FlinkAggregate,
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


class RandomWindow(Window[MeasurementUnit]):
    pass


@pytest.mark.parametrize(
    "window, expected",
    [
        (
            SlidingWindow(window_size=timedelta(seconds=30), window_slide=2),
            pytest.raises(TypeError),
        ),
        (
            RandomWindow(),
            pytest.raises(TypeError),
        ),
    ],
)
def test_bad_windows(window, expected):

    with expected:
        build_flink_window(window)


class MockAccumulator(Accumulator[str, str]):
    """
    Takes str input and accumulates them into a batch array.
    Joins back into a string to produce onto a Kafka topic.
    """

    def __init__(self) -> None:
        self.mock_batch: list[str] = []

    def add(self, value: str) -> Self:
        self.mock_batch.append(value)

        return self

    def get_value(self) -> str:
        return ".".join(self.mock_batch)

    def merge(self, other: Self) -> Self:
        self.mock_batch.extend(other.mock_batch)

        return self


def test_flink_aggregate():

    mock_acc = MockAccumulator
    flink_agg = FlinkAggregate(mock_acc, None)

    mock_acc_instance = flink_agg.create_accumulator()
    assert isinstance(mock_acc_instance, MockAccumulator)

    flink_agg.add("a", mock_acc_instance)
    flink_agg.add("b", mock_acc_instance)
    flink_agg.add("c", mock_acc_instance)

    assert flink_agg.acc.mock_batch == ["a", "b", "c"]

    other_flink_agg = FlinkAggregate(mock_acc)
    other_acc_instance = other_flink_agg.create_accumulator()

    assert isinstance(other_acc_instance, MockAccumulator)

    other_flink_agg.add("d", other_acc_instance)
    other_flink_agg.add("e", other_acc_instance)

    assert other_flink_agg.acc.mock_batch == ["d", "e"]

    merged_acc = flink_agg.merge(flink_agg.acc, other_flink_agg.acc)

    assert merged_acc.mock_batch == ["a", "b", "c", "d", "e"]

    assert flink_agg.get_result(merged_acc) == "a.b.c.d.e"
