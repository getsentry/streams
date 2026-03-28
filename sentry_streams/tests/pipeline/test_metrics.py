from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

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
    configure_metrics,
    get_metrics,
    get_size,
)


def _metric(name: Metric) -> str:
    return name.value


class TestMetric:
    def test_metric_enum_values(self) -> None:
        assert Metric.INPUT_MESSAGES.value == "input.messages"
        assert Metric.INPUT_BYTES.value == "input.bytes"
        assert Metric.OUTPUT_MESSAGES.value == "output.messages"
        assert Metric.OUTPUT_BYTES.value == "output.bytes"
        assert Metric.DURATION.value == "duration"
        assert Metric.ERRORS.value == "errors"


class TestDummyMetricsBackend:
    def test_increment(self) -> None:
        backend = DummyMetricsBackend()
        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
        backend.increment(_metric(Metric.INPUT_MESSAGES), tags={"key": "value"})

    def test_gauge(self) -> None:
        backend = DummyMetricsBackend()
        backend.gauge(_metric(Metric.INPUT_BYTES), 100)
        backend.gauge(_metric(Metric.INPUT_BYTES), 200.5, tags={"key": "value"})

    def test_timing(self) -> None:
        backend = DummyMetricsBackend()
        backend.timing(_metric(Metric.DURATION), 1000)
        backend.timing(_metric(Metric.DURATION), 1500.5, tags={"key": "value"})


class TestMetricsFacade:
    def test_delegates_to_backend(self) -> None:
        inner = MagicMock(spec=DummyMetricsBackend)
        facade = Metrics(inner)
        facade.increment(Metric.INPUT_MESSAGES, 5, tags={"k": "v"})
        inner.increment.assert_called_once_with("input.messages", 5, tags={"k": "v"})
        facade.gauge(Metric.INPUT_BYTES, 42)
        inner.gauge.assert_called_once_with("input.bytes", 42, tags=None)
        facade.timing(Metric.DURATION, 10.0, tags={})
        inner.timing.assert_called_once_with("duration", 10.0, tags={})


class TestDatadogMetricsBackend:
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_init_namespace_is_metrics_prefix(self, mock_dogstatsd: Any) -> None:
        DatadogMetricsBackend("localhost", 8125)
        mock_dogstatsd.assert_called_once_with(
            host="localhost",
            port=8125,
            namespace=METRICS_PREFIX.strip("."),
            constant_tags=[],
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_init_with_tags(self, mock_dogstatsd: Any) -> None:
        tags = {"env": "production", "service": "streams"}
        DatadogMetricsBackend("localhost", 8125, tags=tags)
        mock_dogstatsd.assert_called_once_with(
            host="localhost",
            port=8125,
            namespace=METRICS_PREFIX.strip("."),
            constant_tags=[],
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_increment(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value

        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)

        mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_increment_with_tags(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value

        backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags={"env": "test"})

        mock_client.increment.assert_called_once_with("input.messages", 1, tags=["env:test"])

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_increment_merges_constructor_tags(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, tags={"service": "streams"})
        mock_client = mock_dogstatsd.return_value

        backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags={"env": "test"})

        called_tags = mock_client.increment.call_args[1]["tags"]
        assert set(called_tags) == {"service:streams", "env:test"}

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_gauge(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value

        backend.gauge(_metric(Metric.INPUT_BYTES), 100)

        mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_timing(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value

        backend.timing(_metric(Metric.DURATION), 1500)

        mock_client.timing.assert_called_once_with("duration", 1500, tags=None)


class TestBufferedMetricsBackend:
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_without_auto_flush(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)

        mock_client.increment.assert_not_called()

        backend.flush()

        mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_with_throttling(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.side_effect = [METRICS_FREQUENCY_SEC + 1, METRICS_FREQUENCY_SEC + 2]
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)

        mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_accumulation(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
        backend.increment(_metric(Metric.INPUT_MESSAGES), 3)
        backend.flush()

        mock_client.increment.assert_called_once_with("input.messages", 8, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_gauge(self, mock_dogstatsd: Any) -> None:
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)

        backend.gauge(_metric(Metric.INPUT_BYTES), 100)
        backend.flush()

        mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_gauge_replacement(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

        backend.gauge(_metric(Metric.INPUT_BYTES), 100)
        backend.gauge(_metric(Metric.INPUT_BYTES), 200)
        backend.flush()

        mock_client.gauge.assert_called_once_with("input.bytes", 200, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_flush_all_metric_types(self, mock_dogstatsd: Any) -> None:
        inner = DatadogMetricsBackend("localhost", 8125)
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=60.0)

        backend.increment(_metric(Metric.INPUT_MESSAGES), 5)
        backend.gauge(_metric(Metric.INPUT_BYTES), 100)
        backend.timing(_metric(Metric.DURATION), 1000)

        backend.flush()

        mock_client.increment.assert_called_once_with("input.messages", 5, tags=None)
        mock_client.gauge.assert_called_once_with("input.bytes", 100, tags=None)
        mock_client.timing.assert_called_once_with("duration", 1000, tags=None)

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_buffered_wraps_datadog_with_constructor_tags(
        self, mock_time: Any, mock_dogstatsd: Any
    ) -> None:
        mock_time.return_value = 0.0
        inner = DatadogMetricsBackend("localhost", 8125, tags={"service": "streams"})
        mock_client = mock_dogstatsd.return_value
        backend = BufferedMetricsBackend(inner, throttle_interval_sec=METRICS_FREQUENCY_SEC)

        backend.increment(_metric(Metric.INPUT_MESSAGES), 1, tags={"env": "production"})
        backend.flush()

        called_tags = mock_client.increment.call_args[1]["tags"]
        assert set(called_tags) == {"service:streams", "env:production"}


class TestArroyoMetricsBackend:
    def test_increment(self) -> None:
        inner = Mock(spec=DummyMetricsBackend)
        backend = ArroyoMetricsBackend(inner)

        backend.increment("arroyo.consumer.run.count", 5, {"env": "test"})

        inner.increment.assert_called_once_with(
            "arroyo.consumer.run.count", 5, tags={"env": "test"}
        )

    def test_gauge(self) -> None:
        inner = Mock(spec=DummyMetricsBackend)
        backend = ArroyoMetricsBackend(inner)

        backend.gauge("arroyo.consumer.run.count", 100, {"env": "test"})

        inner.gauge.assert_called_once_with("arroyo.consumer.run.count", 100, tags={"env": "test"})

    def test_timing(self) -> None:
        inner = Mock(spec=DummyMetricsBackend)
        backend = ArroyoMetricsBackend(inner)

        backend.timing("arroyo.consumer.poll.time", 1000, {"env": "test"})

        inner.timing.assert_called_once_with(
            "arroyo.consumer.poll.time", 1000, tags={"env": "test"}
        )

    def test_methods_without_tags_pass_empty_dict(self) -> None:
        inner = Mock(spec=DummyMetricsBackend)
        backend = ArroyoMetricsBackend(inner)

        backend.increment("arroyo.consumer.run.count")
        backend.gauge("arroyo.consumer.run.count", 100)
        backend.timing("arroyo.consumer.poll.time", 1000)

        inner.increment.assert_called_once_with("arroyo.consumer.run.count", 1, tags={})
        inner.gauge.assert_called_once_with("arroyo.consumer.run.count", 100, tags={})
        inner.timing.assert_called_once_with("arroyo.consumer.poll.time", 1000, tags={})


class TestLogMetricsBackend:
    @patch("sentry_streams.metrics.metrics.logger")
    def test_increment_logs_immediately(self, mock_logger: Any) -> None:
        backend = LogMetricsBackend(period_sec=15.0, tags={"env": "test"})
        mock_info = mock_logger.info

        backend.increment(_metric(Metric.INPUT_MESSAGES), 1)

        mock_info.assert_called_once()
        call_msg = mock_info.call_args[0][0]
        assert "input.messages" in call_msg
        assert "env:test" in call_msg

    @patch("sentry_streams.metrics.metrics.logger")
    def test_each_call_emits_separate_log_line(self, mock_logger: Any) -> None:
        backend = LogMetricsBackend(period_sec=60.0)
        mock_info = mock_logger.info

        backend.increment(_metric(Metric.INPUT_MESSAGES), 1)
        backend.increment(_metric(Metric.INPUT_MESSAGES), 2)

        assert mock_info.call_count == 2


class TestBufferedMetricsBackendWithLogBackend:
    @patch("sentry_streams.metrics.metrics.logger")
    @patch("time.time")
    def test_buffer_accumulation_and_flush(self, mock_time: Any, mock_logger: Any) -> None:
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
    def test_flush_logs_and_clears(self, mock_time: Any, mock_logger: Any) -> None:
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
    def test_throttled_flush(self, mock_time: Any, mock_logger: Any) -> None:
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
    def test_global_tags_from_inner_log_backend(self, mock_time: Any, mock_logger: Any) -> None:
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


class TestConfigureMetrics:
    def teardown_method(self) -> None:
        import sentry_streams.metrics.metrics

        sentry_streams.metrics.metrics._metrics_backend = None

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    def test_configure_metrics_dummy(self, mock_arroyo_configure: Any) -> None:
        backend = DummyMetricsBackend()

        configure_metrics(backend)

        from sentry_streams.metrics.metrics import _metrics_backend

        assert _metrics_backend is backend
        mock_arroyo_configure.assert_called_once()
        arroyo_backend = mock_arroyo_configure.call_args[0][0]
        assert isinstance(arroyo_backend, ArroyoMetricsBackend)

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_configure_metrics_datadog(
        self, mock_dogstatsd: Any, mock_arroyo_configure: Any
    ) -> None:
        backend = DatadogMetricsBackend("localhost", 8125)

        configure_metrics(backend)

        from sentry_streams.metrics.metrics import _metrics_backend

        assert _metrics_backend is backend
        mock_arroyo_configure.assert_called_once()

    def test_configure_metrics_already_set(self) -> None:
        backend1 = DummyMetricsBackend()
        backend2 = DummyMetricsBackend()

        configure_metrics(backend1)

        with pytest.raises(AssertionError, match="Metrics is already set"):
            configure_metrics(backend2)

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    def test_configure_metrics_force(self, mock_arroyo_configure: Any) -> None:
        backend1 = DummyMetricsBackend()
        backend2 = DummyMetricsBackend()

        configure_metrics(backend1)
        configure_metrics(backend2, force=True)

        from sentry_streams.metrics.metrics import _metrics_backend

        assert _metrics_backend is backend2

    def test_configure_metrics_invalid_type(self) -> None:
        invalid_backend = "not_a_metrics_backend"

        with pytest.raises(AssertionError):
            configure_metrics(invalid_backend)  # type: ignore[arg-type]


class TestGetMetrics:
    def teardown_method(self) -> None:
        import sentry_streams.metrics.metrics

        sentry_streams.metrics.metrics._metrics_backend = None

    def test_get_metrics_none_configured(self) -> None:
        metrics = get_metrics()
        assert isinstance(metrics, Metrics)
        inner = object.__getattribute__(metrics, "_Metrics__backend")
        assert isinstance(inner, DummyMetricsBackend)

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    def test_get_metrics_configured(self, mock_arroyo_configure: Any) -> None:
        backend = DummyMetricsBackend()
        configure_metrics(backend)

        metrics = get_metrics()
        assert isinstance(metrics, Metrics)
        inner = object.__getattribute__(metrics, "_Metrics__backend")
        assert inner is backend


class TestGetSize:
    def test_get_size_string(self) -> None:
        assert get_size("hello") == 5
        assert get_size("") == 0

    def test_get_size_bytes(self) -> None:
        assert get_size(b"hello") == 5
        assert get_size(b"") == 0

    def test_get_size_other_types(self) -> None:
        assert get_size(123) is None
        assert get_size([1, 2, 3]) is None
        assert get_size({"key": "value"}) is None
