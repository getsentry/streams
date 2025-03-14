from datetime import timedelta
from typing import Self

import pytest
from pyflink.common import Time, Types
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
    translate_custom_type,
    translate_to_flink_type,
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

    acc_instance = flink_agg.create_accumulator()
    assert isinstance(acc_instance, MockAccumulator)

    flink_agg.add("a", acc_instance)
    flink_agg.add("b", acc_instance)
    flink_agg.add("c", acc_instance)

    assert acc_instance.mock_batch == ["a", "b", "c"]

    other_flink_agg = FlinkAggregate(mock_acc, None)
    other_acc_instance = other_flink_agg.create_accumulator()
    assert isinstance(other_acc_instance, MockAccumulator)

    other_flink_agg.add("d", other_acc_instance)
    other_flink_agg.add("e", other_acc_instance)

    assert other_acc_instance.mock_batch == ["d", "e"]

    merged_acc = flink_agg.merge(acc_instance, other_acc_instance)
    assert isinstance(merged_acc, MockAccumulator)

    assert merged_acc.mock_batch == ["a", "b", "c", "d", "e"]

    assert flink_agg.get_result(merged_acc) == "a.b.c.d.e"


@pytest.mark.parametrize(
    "python_type, flink_type",
    [
        (str, Types.STRING()),
        (tuple[str, int], Types.TUPLE([Types.STRING(), Types.INT()])),
        (dict[str, str], Types.MAP(Types.STRING(), Types.STRING())),
    ],
)
def test_flink_type_conversion(python_type, flink_type):

    assert translate_to_flink_type(python_type) == flink_type


class RandomClass:
    pass


@pytest.mark.parametrize(
    "python_type, flink_type",
    [
        (RandomClass(), Types.PICKLED_BYTE_ARRAY()),
    ],
)
def test_flink_custom_type_conversion(python_type, flink_type):

    assert translate_custom_type(python_type) == flink_type
