from __future__ import annotations

import logging
import time
from abc import abstractmethod
from enum import Enum
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from arroyo.utils.metric_defs import MetricName as ArroyoMetricName
from arroyo.utils.metrics import DummyMetricsBackend as ArroyoDummyMetricsBackend
from arroyo.utils.metrics import configure_metrics as arroyo_configure_metrics
from datadog.dogstatsd.base import DogStatsd

Tags = dict[str, str]
logger = logging.getLogger("sentry_streams.metrics.log_backend")


METRICS_FREQUENCY_SEC = 10

# Single source of truth for the metrics namespace used by both Datadog and Log backends.
METRICS_PREFIX = "streams.pipeline"

# max number of (UDP) packets in the dogstatsd queue. 0 means unlimited.
SENDER_QUEUE_SIZE = 100000
# do not block process shutdown on metrics.
SENDER_QUEUE_TIMEOUT = 0


class Metric(Enum):
    # This counts how many messages were input into the step in the pipeline.
    # Tags: step, pipeline
    INPUT_MESSAGES = "input.messages"
    # This counts how many bytes were input into the step in the pipeline.
    # Tags: step, pipeline
    INPUT_BYTES = "input.bytes"
    # This counts how many messages were output from the step in the pipeline. Useful for filter/batch steps.
    # Tags: step, pipeline
    OUTPUT_MESSAGES = "output.messages"
    # This counts how many bytes were output from the step in the pipeline. Useful for filter/batch steps.
    # Tags: step, pipeline
    OUTPUT_BYTES = "output.bytes"
    # This times how long the application code in the step took to run.
    # Tags: step, pipeline
    DURATION = "duration"
    # This counts how many errors were encountered in the step in the pipeline.
    # Tags: step, pipeline, error_type
    ERRORS = "errors"


@runtime_checkable
class Metrics(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self,
        name: Metric,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Sets a gauge metric to the given value.
        """
        raise NotImplementedError

    @abstractmethod
    def timing(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Records a timing metric.
        """
        raise NotImplementedError

    @abstractmethod
    def add_global_tags(self, tags: Tags) -> None:
        """
        Adds global tags to the metrics.
        """
        raise NotImplementedError

    @abstractmethod
    def remove_global_tags(self, tags: Tags) -> None:
        """
        Removes global tags from the metrics.
        """
        raise NotImplementedError


class DummyMetricsBackend(Metrics):
    """
    Default metrics backend that does not record anything.
    """

    def increment(
        self,
        name: Metric,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        pass

    def gauge(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        pass

    def timing(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        pass

    def add_global_tags(self, tags: Tags) -> None:
        pass

    def remove_global_tags(self, tags: Tags) -> None:
        pass


BufferedMetric = tuple[Metric, float, list[str] | None]


@runtime_checkable
class MetricsFlusher(Protocol):
    """
    Protocol for flushing buffered metrics to a destination (e.g. Datadog or log).
    """

    def flush(
        self,
        timers: Iterable[BufferedMetric],
        counters: Iterable[BufferedMetric],
        gauges: Iterable[BufferedMetric],
    ) -> None:
        """Write the buffered metrics to the destination."""
        ...


class DatadogFlusher:
    """Flusher that sends buffered metrics to a DogStatsd client."""

    def __init__(self, client: DogStatsd) -> None:
        self.__client = client

    def flush(
        self,
        timers: Iterable[BufferedMetric],
        counters: Iterable[BufferedMetric],
        gauges: Iterable[BufferedMetric],
    ) -> None:
        for name, value, tags in timers:
            self.__client.timing(name.value, value, tags=tags or [])
        for name, value, tags in counters:
            self.__client.increment(name.value, value, tags=tags or [])
        for name, value, tags in gauges:
            self.__client.gauge(name.value, value, tags=tags or [])


class LogFlusher:
    """Flusher that writes buffered metrics to a logger."""

    def __init__(self, prefix: str) -> None:
        self.__prefix = prefix

    def flush(
        self,
        timers: Iterable[BufferedMetric],
        counters: Iterable[BufferedMetric],
        gauges: Iterable[BufferedMetric],
    ) -> None:
        parts: list[str] = [self.__prefix]
        for name, value, tags in timers:
            tags_str = " ".join(tags) if tags else ""
            parts.append(f"timing {name.value}={value} {tags_str}".strip())
        for name, value, tags in counters:
            tags_str = " ".join(tags) if tags else ""
            parts.append(f"counter {name.value}={value} {tags_str}".strip())
        for name, value, tags in gauges:
            tags_str = " ".join(tags) if tags else ""
            parts.append(f"gauge {name.value}={value} {tags_str}".strip())
        if len(parts) > 1:
            logger.info(" | ".join(parts))
        else:
            logger.info("No metrics to flush")


class BufferedMetricsBackend(Metrics):
    """
    Metrics backend that buffers updates and periodically flushes them
    via an injected flusher (e.g. Datadog or log).
    """

    def __init__(
        self,
        flusher: MetricsFlusher,
        throttle_interval_sec: float,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__flusher = flusher
        self.__throttle_interval_sec = throttle_interval_sec
        self.tags = tags
        self.__normalized_tags = self.__normalize_tags(tags) if tags is not None else []
        self.__timers: dict[int, BufferedMetric] = {}
        self.__counters: dict[int, BufferedMetric] = {}
        self.__gauges: dict[int, BufferedMetric] = {}
        self.__last_flush_time = 0.0

    def __add_to_buffer(
        self,
        buffer: dict[int, BufferedMetric],
        name: Metric,
        value: Union[int, float],
        tags: Optional[Tags] = None,
        replace: bool = False,
    ) -> None:
        if tags is None:
            key = hash(name)
            normalized_tags = self.__normalized_tags
        else:
            normalized_tags = self.__normalize_tags(tags) + self.__normalized_tags
            key = hash((name, frozenset(normalized_tags)))

        if key in buffer:
            new_value = buffer[key][1] + value if not replace else value
            buffer[key] = (name, new_value, normalized_tags)
        else:
            buffer[key] = (name, value, normalized_tags)

    def __normalize_tags(self, tags: Tags) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def increment(
        self,
        name: Metric,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__add_to_buffer(self.__counters, name, value, tags)
        self.__throttled_flush()

    def gauge(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__add_to_buffer(self.__gauges, name, value, tags, replace=True)
        self.__throttled_flush()

    def timing(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__add_to_buffer(self.__timers, name, value, tags)
        self.__throttled_flush()

    def add_global_tags(self, tags: Tags) -> None:
        if self.tags is None:
            self.tags = tags
        else:
            self.tags.update(tags)

        self.__normalized_tags = self.__normalize_tags(self.tags)

    def remove_global_tags(self, tags: Tags) -> None:
        if self.tags:
            for tag in tags:
                self.tags.pop(tag, None)
            self.__normalized_tags = self.__normalize_tags(self.tags)

    def __throttled_flush(self) -> None:
        if time.time() - self.__last_flush_time >= self.__throttle_interval_sec:
            self.flush()

    def flush(self) -> None:
        self.__flusher.flush(
            self.__timers.values(),
            self.__counters.values(),
            self.__gauges.values(),
        )
        self.__reset()

    def __reset(self) -> None:
        self.__timers.clear()
        self.__counters.clear()
        self.__gauges.clear()
        self.__last_flush_time = time.time()


class DatadogMetricsBackend(BufferedMetricsBackend):
    """
    Datadog metrics backend. Buffers metrics and flushes to DogStatsd.
    """

    def __init__(
        self,
        host: str,
        port: int,
        prefix: str,
        tags: Optional[Tags] = None,
        udp_queue_size: Optional[int] = None,
    ) -> None:
        # Do not pass constant_tags to DogStatsd: BufferedMetricsBackend already
        # adds tags to each metric. Passing both would duplicate tags.
        self.datadog_client = DogStatsd(
            host=host,
            port=port,
            namespace=prefix.strip("."),
            constant_tags=[],
        )
        # ignore mypy because that method just is untyped, yet part of public API
        self.datadog_client.enable_background_sender(  # type: ignore[no-untyped-call]
            sender_queue_size=udp_queue_size if udp_queue_size is not None else SENDER_QUEUE_SIZE,
            sender_queue_timeout=SENDER_QUEUE_TIMEOUT,
        )
        flusher = DatadogFlusher(self.datadog_client)
        super().__init__(flusher, METRICS_FREQUENCY_SEC, tags)


class LogMetricsBackend(BufferedMetricsBackend):
    """
    Metrics backend that buffers metrics and periodically logs accumulated
    values at a configurable interval.
    """

    def __init__(
        self,
        period_sec: float,
        tags: Optional[Tags] = None,
    ) -> None:
        flusher = LogFlusher(METRICS_PREFIX.strip("."))
        super().__init__(flusher, period_sec, tags)


class ArroyoDatadogMetricsBackend:
    """
    Arroyo wrapper around Datadog metrics backend.
    """

    def __init__(self, datadog_client: DogStatsd) -> None:
        self.__datadog_client = datadog_client

    def __normalize_tags(self, tags: Mapping[str, str]) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def increment(
        self,
        name: ArroyoMetricName,
        value: Union[int, float] = 1,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__datadog_client.increment(
            name, value, tags=self.__normalize_tags(tags) if tags else None
        )

    def gauge(
        self,
        name: ArroyoMetricName,
        value: Union[int, float],
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__datadog_client.gauge(name, value, tags=self.__normalize_tags(tags) if tags else None)

    def timing(
        self,
        name: ArroyoMetricName,
        value: Union[int, float],
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__datadog_client.timing(
            name, value, tags=self.__normalize_tags(tags) if tags else None
        )


_metrics_backend: Optional[Metrics] = None
_dummy_metrics_backend = DummyMetricsBackend()


def configure_metrics(metrics: Metrics, force: bool = False) -> None:
    """
    Metrics can generally only be configured once, unless force is passed
    on subsequent initializations.
    """
    global _metrics_backend

    if not force:
        assert _metrics_backend is None, "Metrics is already set"

    # Perform a runtime check of metrics instance upon initialization of
    # this class to avoid errors down the line when it is used.
    assert isinstance(metrics, Metrics)
    _metrics_backend = metrics
    if isinstance(metrics, DatadogMetricsBackend):
        arroyo_configure_metrics(ArroyoDatadogMetricsBackend(metrics.datadog_client))
    else:
        arroyo_configure_metrics(ArroyoDummyMetricsBackend())


def get_metrics() -> Metrics:
    if _metrics_backend is None:
        return _dummy_metrics_backend
    return _metrics_backend


def get_size(obj: Any) -> int | None:
    # TODO: Make this work for all types
    if isinstance(obj, (str, bytes)):
        return len(obj)
    return None
