from collections.abc import Generator
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

import sentry_streams.metrics.metrics as metrics_module
from sentry_streams.metrics.metrics import (
    METRICS_FREQUENCY_SEC,
    METRICS_PREFIX,
    ArroyoMetricsBackend,
    BufferedMetricsBackend,
    DatadogMetricsBackend,
    DummyMetricsBackend,
    LogMetricsBackend,
    Metric,
    Metrics,
    MetricsBackend,
    configure_metrics,
)


def _metric(name: Metric) -> str:
    return name.value


def _buffered_inner_backend(buffered: BufferedMetricsBackend) -> MetricsBackend:
    return cast(
        MetricsBackend,
        object.__getattribute__(buffered, "_BufferedMetricsBackend__backend"),
    )


@pytest.fixture(autouse=True)
def reset_metrics_backend() -> Generator[None, None, None]:
    metrics_module._metrics_backend = None
    yield
    metrics_module._metrics_backend = None


def test_metric_enum_values() -> None:
    assert Metric.INPUT_MESSAGES.value == "input.messages"
    assert Metric.INPUT_BYTES.value == "input.bytes"
    assert Metric.OUTPUT_MESSAGES.value == "output.messages"
    assert Metric.OUTPUT_BYTES.value == "output.bytes"
    assert Metric.DURATION.value == "duration"
    assert Metric.ERRORS.value == "errors"


def test_dummy_metrics_backend_increment() -> None:
    backend = DummyMetricsBackend()
    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
    backend.increment(_metric(Metric.INPUT_MESSAGES), tags={"key": "value"})


def test_dummy_metrics_backend_gauge() -> None:
    backend = DummyMetricsBackend()
    backend.gauge(_metric(Metric.INPUT_BYTES), 100)
    backend.gauge(_metric(Metric.INPUT_BYTES), 200.5, tags={"key": "value"})


def test_dummy_metrics_backend_timing() -> None:
    backend = DummyMetricsBackend()
    backend.timing(_metric(Metric.DURATION), 1000)
    backend.timing(_metric(Metric.DURATION), 1500.5, tags={"key": "value"})


def test_metrics_facade_delegates_to_backend() -> None:
    inner = MagicMock(spec=DummyMetricsBackend)
    facade = Metrics(inner)
    facade.increment(Metric.INPUT_MESSAGES, 5, tags={"k": "v"})
    inner.increment.assert_called_once_with("input.messages", 5, tags={"k": "v"})
    facade.gauge(Metric.INPUT_BYTES, 42)
    inner.gauge.assert_called_once_with("input.bytes", 42, tags=None)
    facade.timing(Metric.DURATION, 10.0, tags={})
    inner.timing.assert_called_once_with("duration", 10.0, tags={})


@patch("sentry_streams.metrics.metrics.DogStatsd")
def test_datadog_init_namespace_is_metrics_prefix(mock_dogstatsd: Any) -> None:
    DatadogMetricsBackend("localhost", 8125)
    mock_dogstatsd.assert_called_once_with(
        host="localhost",
        port=8125,
        namespace=METRICS_PREFIX.strip("."),
        constant_tags=[],
    )


@patch("sentry_streams.metrics.metrics.DogStatsd")
def test_datadog_increment(mock_dogstatsd: Any) -> None:
    backend = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value

    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
    backend.gauge(_metric(Metric.INPUT_BYTES), 100)
    backend.timing(_metric(Metric.DURATION), 1500)

    mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)
    mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=None)
    mock_client.timing.assert_called_once_with("duration", 1500, tags=None)


@patch("sentry_streams.metrics.metrics.DogStatsd")
def test_datadog_increment_with_tags(mock_dogstatsd: Any) -> None:
    backend = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value
    tags = {"env": "test"}

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags=tags)
    backend.gauge(_metric(Metric.INPUT_BYTES), 100, tags=tags)
    backend.timing(_metric(Metric.DURATION), 1500, tags=tags)

    mock_client.increment.assert_called_once_with("input.messages", 1, tags=["env:test"])
    mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=["env:test"])
    mock_client.timing.assert_called_once_with("duration", 1500, tags=["env:test"])


@patch("sentry_streams.metrics.metrics.DogStatsd")
def test_datadog_increment_merges_constructor_tags(mock_dogstatsd: Any) -> None:
    backend = DatadogMetricsBackend("localhost", 8125, tags={"service": "streams"})
    mock_client = mock_dogstatsd.return_value
    tags = {"env": "test"}

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags=tags)
    backend.gauge(_metric(Metric.INPUT_BYTES), 100, tags=tags)
    backend.timing(_metric(Metric.DURATION), 1500, tags=tags)

    expected = {"service:streams", "env:test"}
    mock_client.increment.assert_called_once()
    assert mock_client.increment.call_args[0] == ("input.messages", 1)
    assert set(mock_client.increment.call_args[1]["tags"]) == expected
    mock_client.gauge.assert_called_once()
    assert mock_client.gauge.call_args[0] == ("input.bytes", 100)
    assert set(mock_client.gauge.call_args[1]["tags"]) == expected
    mock_client.timing.assert_called_once()
    assert mock_client.timing.call_args[0] == ("duration", 1500)
    assert set(mock_client.timing.call_args[1]["tags"]) == expected


@patch("sentry_streams.metrics.metrics.DogStatsd")
@patch("time.time")
def test_buffered_increment_with_throttling(mock_time: Any, mock_dogstatsd: Any) -> None:
    mock_time.side_effect = [METRICS_FREQUENCY_SEC + 1, METRICS_FREQUENCY_SEC + 2]
    inner = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)

    mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)


@patch("sentry_streams.metrics.metrics.DogStatsd")
@patch("time.time")
def test_buffered_increment_accumulation(mock_time: Any, mock_dogstatsd: Any) -> None:
    mock_time.return_value = 0.0
    inner = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
    backend.increment(_metric(Metric.INPUT_MESSAGES), 3)
    backend.flush()

    mock_client.increment.assert_called_once_with("input.messages", 8, tags=None)


@patch("sentry_streams.metrics.metrics.DogStatsd")
@patch("time.time")
def test_buffered_gauge_replacement(mock_time: Any, mock_dogstatsd: Any) -> None:
    mock_time.return_value = 0.0
    inner = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

    backend.gauge(_metric(Metric.INPUT_BYTES), 100)
    backend.gauge(_metric(Metric.INPUT_BYTES), 200)
    backend.flush()

    mock_client.gauge.assert_called_once_with("input.bytes", 200, tags=None)


@patch("sentry_streams.metrics.metrics.DogStatsd")
@patch("time.time")
def test_buffered_flush_all_metric_types(mock_time: Any, mock_dogstatsd: Any) -> None:
    mock_time.return_value = 0.0
    inner = DatadogMetricsBackend("localhost", 8125)
    mock_client = mock_dogstatsd.return_value
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)

    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
    backend.gauge(_metric(Metric.INPUT_BYTES), 100)
    backend.timing(_metric(Metric.DURATION), 1000)

    mock_client.increment.assert_not_called()
    mock_client.gauge.assert_not_called()
    mock_client.timing.assert_not_called()

    backend.flush()

    mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)
    mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=None)
    mock_client.timing.assert_called_once_with("duration", 1000, tags=None)


@patch("sentry_streams.metrics.metrics.DogStatsd")
@patch("time.time")
def test_buffered_wraps_datadog_with_constructor_tags(mock_time: Any, mock_dogstatsd: Any) -> None:
    mock_time.return_value = 0.0
    inner = DatadogMetricsBackend("localhost", 8125, tags={"service": "streams"})
    mock_client = mock_dogstatsd.return_value
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags={"env": "production"})
    backend.flush()

    called_tags = mock_client.increment.call_args[1]["tags"]
    assert set(called_tags) == {"service:streams", "env:production"}


def test_arroyo_delegates_increment_gauge_timing_with_tags() -> None:
    inner = Mock(spec=DummyMetricsBackend)
    backend = ArroyoMetricsBackend(inner)
    tags = {"env": "test"}

    backend.increment("arroyo.consumer.run.count", 5, tags)
    backend.gauge("arroyo.consumer.run.count", 100, tags)
    backend.timing("arroyo.consumer.poll.time", 1000, tags)

    inner.increment.assert_called_once_with("arroyo.consumer.run.count", 5, tags={"env": "test"})
    inner.gauge.assert_called_once_with("arroyo.consumer.run.count", 100, tags={"env": "test"})
    inner.timing.assert_called_once_with("arroyo.consumer.poll.time", 1000, tags={"env": "test"})


def test_arroyo_methods_without_tags_pass_empty_dict() -> None:
    inner = Mock(spec=DummyMetricsBackend)
    backend = ArroyoMetricsBackend(inner)

    backend.increment("arroyo.consumer.run.count")
    backend.gauge("arroyo.consumer.run.count", 100)
    backend.timing("arroyo.consumer.poll.time", 1000)

    inner.increment.assert_called_once_with("arroyo.consumer.run.count", 1, tags={})
    inner.gauge.assert_called_once_with("arroyo.consumer.run.count", 100, tags={})
    inner.timing.assert_called_once_with("arroyo.consumer.poll.time", 1000, tags={})


@patch("sentry_streams.metrics.metrics.logger")
def test_log_increment_logs_immediately(mock_logger: Any) -> None:
    backend = LogMetricsBackend(period_sec=15.0, tags={"env": "test"})
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1)

    mock_info.assert_called_once()
    call_msg = mock_info.call_args[0][0]
    assert "input.messages" in call_msg
    assert "env:test" in call_msg


@patch("sentry_streams.metrics.metrics.logger")
def test_log_each_call_emits_separate_log_line(mock_logger: Any) -> None:
    backend = LogMetricsBackend(period_sec=60.0)
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1)
    backend.increment(_metric(Metric.INPUT_MESSAGES), 2)

    assert mock_info.call_count == 2


@patch("sentry_streams.metrics.metrics.logger")
@patch("time.time")
def test_buffered_log_accumulation_and_flush(mock_time: Any, mock_logger: Any) -> None:
    mock_time.return_value = 0.0
    inner = LogMetricsBackend(period_sec=60.0)
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
    backend.increment(_metric(Metric.INPUT_MESSAGES), 3)
    backend.gauge(_metric(Metric.INPUT_BYTES), 100)
    backend.gauge(_metric(Metric.INPUT_BYTES), 200)
    backend.timing(_metric(Metric.DURATION), 100)
    backend.timing(_metric(Metric.DURATION), 50)
    backend.flush()

    assert mock_info.call_count == 3
    logged = [c[0][0] for c in mock_info.call_args_list]
    assert any("timing" in m and "duration" in m and "150" in m for m in logged)
    assert any("counter" in m and "input.messages" in m and "8" in m for m in logged)
    assert any("gauge" in m and "input.bytes" in m and "200" in m for m in logged)


@patch("sentry_streams.metrics.metrics.logger")
@patch("time.time")
def test_buffered_log_flush_logs_and_clears(mock_time: Any, mock_logger: Any) -> None:
    mock_time.return_value = 0.0
    inner = LogMetricsBackend(period_sec=60.0)
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1)
    backend.flush()

    mock_info.assert_called_once()
    call_msg = mock_info.call_args[0][0]
    assert METRICS_PREFIX.split(".")[0] in call_msg
    assert "input.messages" in call_msg

    mock_info.reset_mock()
    backend.increment(_metric(Metric.INPUT_MESSAGES), 2)
    backend.flush()
    mock_info.assert_called_once()
    call_msg = mock_info.call_args[0][0]
    assert "2" in call_msg


@patch("sentry_streams.metrics.metrics.logger")
@patch("time.time")
def test_buffered_log_throttled_flush(mock_time: Any, mock_logger: Any) -> None:
    mock_time.return_value = 0.0
    inner = LogMetricsBackend(period_sec=60.0)
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=10.0)
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1)
    mock_info.assert_not_called()

    mock_time.return_value = 11.0
    backend.increment(_metric(Metric.INPUT_MESSAGES), 1)
    mock_info.assert_called_once()


@patch("sentry_streams.metrics.metrics.logger")
@patch("time.time")
def test_buffered_log_global_tags_from_inner(mock_time: Any, mock_logger: Any) -> None:
    mock_time.return_value = 0.0
    inner = LogMetricsBackend(period_sec=60.0, tags={"service": "streams"})
    backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)
    mock_info = mock_logger.info

    backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags={"env": "production"})
    backend.flush()

    mock_info.assert_called_once()
    call_msg = mock_info.call_args[0][0]
    assert "service:streams" in call_msg
    assert "env:production" in call_msg


@patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
def test_configure_metrics_dummy(mock_arroyo_configure: Any) -> None:
    backend = DummyMetricsBackend()

    configure_metrics(backend)

    wrapped = metrics_module._metrics_backend
    assert isinstance(wrapped, BufferedMetricsBackend)
    assert _buffered_inner_backend(wrapped) is backend
    assert (
        object.__getattribute__(wrapped, "_BufferedMetricsBackend__throttle_interval_sec")
        == METRICS_FREQUENCY_SEC
    )
    mock_arroyo_configure.assert_called_once()
    arroyo_backend = mock_arroyo_configure.call_args[0][0]
    assert isinstance(arroyo_backend, ArroyoMetricsBackend)
    assert object.__getattribute__(arroyo_backend, "_ArroyoMetricsBackend__backend") is wrapped


@patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
@patch("sentry_streams.metrics.metrics.DogStatsd")
def test_configure_metrics_datadog(mock_dogstatsd: Any, mock_arroyo_configure: Any) -> None:
    backend = DatadogMetricsBackend("localhost", 8125)

    configure_metrics(backend)

    wrapped = metrics_module._metrics_backend
    assert isinstance(wrapped, BufferedMetricsBackend)
    assert _buffered_inner_backend(wrapped) is backend
    mock_arroyo_configure.assert_called_once()


def test_configure_metrics_already_set() -> None:
    backend1 = DummyMetricsBackend()
    backend2 = DummyMetricsBackend()

    configure_metrics(backend1)

    with pytest.raises(AssertionError, match="Metrics is already set"):
        configure_metrics(backend2)


@patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
def test_configure_metrics_force(mock_arroyo_configure: Any) -> None:
    backend1 = DummyMetricsBackend()
    backend2 = DummyMetricsBackend()

    configure_metrics(backend1)
    configure_metrics(backend2, force=True)

    wrapped = metrics_module._metrics_backend
    assert isinstance(wrapped, BufferedMetricsBackend)
    assert _buffered_inner_backend(wrapped) is backend2
