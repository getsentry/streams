from typing import Any
from unittest.mock import Mock, patch

import pytest

from sentry_streams.metrics.metrics import (
    METRICS_FREQUENCY_SEC,
    ArroyoDatadogMetricsBackend,
    DatadogMetricsBackend,
    DummyMetricsBackend,
    Metric,
    configure_metrics,
    get_metrics,
    get_size,
)


class TestMetric:
    def test_metric_enum_values(self) -> None:
        assert Metric.INPUT_MESSAGES.value == "streams.pipeline.input.messages"
        assert Metric.INPUT_BYTES.value == "streams.pipeline.input.bytes"
        assert Metric.OUTPUT_MESSAGES.value == "streams.pipeline.output.messages"
        assert Metric.OUTPUT_BYTES.value == "streams.pipeline.output.bytes"
        assert Metric.DURATION.value == "streams.pipeline.duration"
        assert Metric.ERRORS.value == "streams.pipeline.errors"


class TestDummyMetricsBackend:
    def test_increment(self) -> None:
        backend = DummyMetricsBackend()
        backend.increment(Metric.INPUT_MESSAGES, 5)
        backend.increment(Metric.INPUT_MESSAGES, tags={"key": "value"})

    def test_gauge(self) -> None:
        backend = DummyMetricsBackend()
        backend.gauge(Metric.INPUT_BYTES, 100)
        backend.gauge(Metric.INPUT_BYTES, 200.5, tags={"key": "value"})

    def test_timing(self) -> None:
        backend = DummyMetricsBackend()
        backend.timing(Metric.DURATION, 1000)
        backend.timing(Metric.DURATION, 1500.5, tags={"key": "value"})

    def test_add_global_tags(self) -> None:
        backend = DummyMetricsBackend()
        backend.add_global_tags({"env": "test"})

    def test_remove_global_tags(self) -> None:
        backend = DummyMetricsBackend()
        backend.remove_global_tags({"env": "test"})


class TestDatadogMetricsBackend:
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_init_with_prefix_dot(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test.")
        assert backend.prefix == "test."
        mock_dogstatsd.assert_called_once_with(
            host="localhost",
            port=8125,
            namespace="test.",
            constant_tags=[],
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_init_without_prefix_dot(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        assert backend.prefix == "test."

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_init_with_tags(self, mock_dogstatsd: Any) -> None:
        tags = {"env": "production", "service": "streams"}
        DatadogMetricsBackend("localhost", 8125, "test", tags)

        expected_tags = ["env:production", "service:streams"]
        mock_dogstatsd.assert_called_once_with(
            host="localhost",
            port=8125,
            namespace="test.",
            constant_tags=expected_tags,
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_without_auto_flush(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.increment(Metric.INPUT_MESSAGES, 5)

        mock_client.increment.assert_not_called()

        backend.flush()

        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 5, tags=[]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_with_throttling(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.side_effect = [METRICS_FREQUENCY_SEC + 1, METRICS_FREQUENCY_SEC + 2]
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.increment(Metric.INPUT_MESSAGES, 5)

        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 5, tags=[]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_increment_with_tags(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value
        tags = {"env": "test"}

        backend.increment(Metric.INPUT_MESSAGES, 1, tags)
        backend.flush()

        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 1, tags=["env:test"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_increment_accumulation(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.increment(Metric.INPUT_MESSAGES, 5)
        backend.increment(Metric.INPUT_MESSAGES, 3)
        backend.flush()

        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 8, tags=[]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_gauge(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.gauge(Metric.INPUT_BYTES, 100)
        backend.flush()

        mock_client.gauge.assert_called_once_with("test.streams.pipeline.input.bytes", 100, tags=[])

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    @patch("time.time")
    def test_gauge_replacement(self, mock_time: Any, mock_dogstatsd: Any) -> None:
        mock_time.return_value = 0.0
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.gauge(Metric.INPUT_BYTES, 100)
        backend.gauge(Metric.INPUT_BYTES, 200)
        backend.flush()

        mock_client.gauge.assert_called_once_with("test.streams.pipeline.input.bytes", 200, tags=[])

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_timing(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.timing(Metric.DURATION, 1500)
        backend.flush()

        mock_client.timing.assert_called_once_with("test.streams.pipeline.duration", 1500, tags=[])

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_add_global_tags_new(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value
        tags = {"env": "production"}

        backend.add_global_tags(tags)
        backend.increment(Metric.INPUT_MESSAGES, 1)
        backend.flush()

        assert backend.tags == tags
        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 1, tags=["env:production"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_add_global_tags_existing(self, mock_dogstatsd: Any) -> None:
        initial_tags = {"service": "streams"}
        backend = DatadogMetricsBackend("localhost", 8125, "test", initial_tags)
        mock_client = mock_dogstatsd.return_value
        new_tags = {"env": "production"}

        backend.add_global_tags(new_tags)
        backend.increment(Metric.INPUT_MESSAGES, 1)
        backend.flush()

        assert backend.tags == {"service": "streams", "env": "production"}
        mock_client.increment.assert_called_once()
        called_args = mock_client.increment.call_args
        assert "service:streams" in called_args[1]["tags"]
        assert "env:production" in called_args[1]["tags"]

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_remove_global_tags(self, mock_dogstatsd: Any) -> None:
        initial_tags = {"service": "streams", "env": "production"}
        backend = DatadogMetricsBackend("localhost", 8125, "test", initial_tags)
        mock_client = mock_dogstatsd.return_value

        backend.remove_global_tags({"env": "production"})
        backend.increment(Metric.INPUT_MESSAGES, 1)
        backend.flush()

        assert backend.tags == {"service": "streams"}
        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 1, tags=["service:streams"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_remove_global_tags_nonexistent(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")

        backend.remove_global_tags({"nonexistent": "tag"})

        assert backend.tags is None

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_flush_all_metric_types(self, mock_dogstatsd: Any) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")
        mock_client = mock_dogstatsd.return_value

        backend.increment(Metric.INPUT_MESSAGES, 5)
        backend.gauge(Metric.INPUT_BYTES, 100)
        backend.timing(Metric.DURATION, 1000)

        backend.flush()

        mock_client.increment.assert_called_once_with(
            "test.streams.pipeline.input.messages", 5, tags=[]
        )
        mock_client.gauge.assert_called_once_with("test.streams.pipeline.input.bytes", 100, tags=[])
        mock_client.timing.assert_called_once_with("test.streams.pipeline.duration", 1000, tags=[])


class TestArroyoDatadogMetricsBackend:
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_increment(self, mock_dogstatsd: Any) -> None:
        mock_client = Mock()
        backend = ArroyoDatadogMetricsBackend(mock_client)

        # Use a valid Arroyo metric name instead of "test.metric"
        backend.increment("arroyo.consumer.run.count", 5, {"env": "test"})

        mock_client.increment.assert_called_once_with(
            "arroyo.consumer.run.count", 5, tags=["env:test"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_gauge(self, mock_dogstatsd: Any) -> None:
        mock_client = Mock()
        backend = ArroyoDatadogMetricsBackend(mock_client)

        # Use a valid Arroyo metric name instead of "test.metric"
        backend.gauge("arroyo.consumer.run.count", 100, {"env": "test"})

        mock_client.gauge.assert_called_once_with(
            "arroyo.consumer.run.count", 100, tags=["env:test"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_timing(self, mock_dogstatsd: Any) -> None:
        mock_client = Mock()
        backend = ArroyoDatadogMetricsBackend(mock_client)

        # Use a valid Arroyo metric name instead of "test.metric"
        backend.timing("arroyo.consumer.poll.time", 1000, {"env": "test"})

        mock_client.timing.assert_called_once_with(
            "arroyo.consumer.poll.time", 1000, tags=["env:test"]
        )

    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_methods_without_tags(self, mock_dogstatsd: Any) -> None:
        mock_client = Mock()
        backend = ArroyoDatadogMetricsBackend(mock_client)

        # Use valid Arroyo metric names instead of "test.metric"
        backend.increment("arroyo.consumer.run.count")
        backend.gauge("arroyo.consumer.run.count", 100)
        backend.timing("arroyo.consumer.poll.time", 1000)

        mock_client.increment.assert_called_once_with("arroyo.consumer.run.count", 1, tags=None)
        mock_client.gauge.assert_called_once_with("arroyo.consumer.run.count", 100, tags=None)
        mock_client.timing.assert_called_once_with("arroyo.consumer.poll.time", 1000, tags=None)


class TestConfigureMetrics:
    def teardown_method(self) -> None:
        import sentry_streams.metrics.metrics

        sentry_streams.metrics.metrics._metrics_backend = None

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    def test_configure_metrics_dummy(self, mock_arroyo_configure: Any) -> None:
        backend = DummyMetricsBackend()

        configure_metrics(backend)

        from sentry_streams.metrics.metrics import _metrics_backend

        assert _metrics_backend == backend
        mock_arroyo_configure.assert_called_once()

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    @patch("sentry_streams.metrics.metrics.DogStatsd")
    def test_configure_metrics_datadog(
        self, mock_dogstatsd: Any, mock_arroyo_configure: Any
    ) -> None:
        backend = DatadogMetricsBackend("localhost", 8125, "test")

        configure_metrics(backend)

        from sentry_streams.metrics.metrics import _metrics_backend

        assert _metrics_backend == backend
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

        assert _metrics_backend == backend2

    def test_configure_metrics_invalid_type(self) -> None:
        invalid_backend = "not_a_metrics_backend"

        with pytest.raises(AssertionError):
            configure_metrics(invalid_backend)  # type: ignore


class TestGetMetrics:
    def teardown_method(self) -> None:
        import sentry_streams.metrics.metrics

        sentry_streams.metrics.metrics._metrics_backend = None

    def test_get_metrics_none_configured(self) -> None:
        metrics = get_metrics()
        assert isinstance(metrics, DummyMetricsBackend)

    @patch("sentry_streams.metrics.metrics.arroyo_configure_metrics")
    def test_get_metrics_configured(self, mock_arroyo_configure: Any) -> None:
        backend = DummyMetricsBackend()
        configure_metrics(backend)

        metrics = get_metrics()
        assert metrics == backend


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
