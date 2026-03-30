from __future__ import annotations

import logging
import time
from abc import abstractmethod
from enum import Enum
from typing import (
    Any,
    Mapping,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from arroyo.utils.metric_defs import MetricName as ArroyoMetricName
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


class Metrics:
    """
    An abstract class that defines the interface for metrics backends.
    """

    def __init__(self, backend: MetricsBackend) -> None:
        self.__backend = backend

    def increment(
        self,
        name: Metric,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        self.__backend.increment(name.value, value, tags=tags)

    def gauge(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Sets a gauge metric to the given value.
        """
        self.__backend.gauge(name.value, value, tags=tags)

    def timing(self, name: Metric, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Records a timing metric.
        """
        self.__backend.timing(name.value, value, tags=tags)


@runtime_checkable
class MetricsBackend(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self,
        name: str,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Sets a gauge metric to the given value.
        """
        raise NotImplementedError

    @abstractmethod
    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        """
        Records a timing metric.
        """
        raise NotImplementedError


class DummyMetricsBackend(MetricsBackend):
    """
    Default metrics backend that does not record anything.
    """

    def increment(
        self,
        name: str,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        pass

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        pass

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        pass


def _combine_tags(base: Tags, tags: Optional[Tags] = None) -> Tags:
    if tags is None:
        return base
    return {
        **base,
        **tags,
    }


class DatadogMetricsBackend(MetricsBackend):
    """
    Datadog metrics backend.
    """

    def __init__(
        self,
        host: str,
        port: int,
        tags: Optional[Tags] = None,
        udp_queue_size: Optional[int] = None,
    ) -> None:
        # Do not pass constant_tags to DogStatsd: BufferedMetricsBackend already
        # adds tags to each metric. Passing both would duplicate tags.
        self.datadog_client = DogStatsd(
            host=host,
            port=port,
            namespace=METRICS_PREFIX.strip("."),
            constant_tags=[],
        )
        # ignore mypy because that method just is untyped, yet part of public API
        self.datadog_client.enable_background_sender(  # type: ignore[no-untyped-call]
            sender_queue_size=udp_queue_size if udp_queue_size is not None else SENDER_QUEUE_SIZE,
            sender_queue_timeout=SENDER_QUEUE_TIMEOUT,
        )
        self.__tags: Tags = tags if tags is not None else {}

    def __normalize_tags(self, tags: Tags) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.datadog_client.increment(
            name,
            value,
            tags=self.__normalize_tags(_combine_tags(self.__tags, tags)) if tags else None,
        )

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.datadog_client.gauge(
            name,
            value,
            tags=self.__normalize_tags(_combine_tags(self.__tags, tags)) if tags else None,
        )

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.datadog_client.timing(
            name,
            value,
            tags=self.__normalize_tags(_combine_tags(self.__tags, tags)) if tags else None,
        )


class LogMetricsBackend(MetricsBackend):
    """
    Metrics backend that logs each update immediately, using the same segment format
    as LogFlusher: ``prefix | counter|gauge|timing name=value tag1:val1 ...``.
    """

    def __init__(self, period_sec: float, tags: Optional[Tags] = None) -> None:
        self.__prefix = METRICS_PREFIX.strip(".")
        self.__base_tags: Tags = tags if tags is not None else {}
        self._period_sec = period_sec

    @staticmethod
    def __normalize_tags(tags: Tags) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def __tag_strings(self, tags: Optional[Tags]) -> list[str]:
        return self.__normalize_tags(_combine_tags(self.__base_tags, tags))

    def __emit(
        self, kind: str, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        tag_list = self.__tag_strings(tags)
        tags_str = " ".join(tag_list) if tag_list else ""
        parts = [self.__prefix, f"{kind} {name}={value} {tags_str}".strip()]
        logger.info(" | ".join(parts))

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.__emit("counter", name, value, tags)

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__emit("gauge", name, value, tags)

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__emit("timing", name, value, tags)


BufferedMetric = tuple[str, float, Tags]


class BufferedMetricsBackend(MetricsBackend):
    """
    Metrics backend that buffers updates and periodically flushes them
    via an injected flusher (e.g. Datadog or log).
    """

    def __init__(
        self,
        backend: MetricsBackend,
        throttle_interval_sec: float,
    ) -> None:
        self.__throttle_interval_sec = throttle_interval_sec
        self.__timers: dict[int, BufferedMetric] = {}
        self.__counters: dict[int, BufferedMetric] = {}
        self.__gauges: dict[int, BufferedMetric] = {}
        self.__last_flush_time = 0.0
        self.__backend = backend

    def __add_to_buffer(
        self,
        buffer: dict[int, BufferedMetric],
        name: str,
        value: Union[int, float],
        tags: Tags,
        replace: bool = False,
    ) -> None:
        if tags is None:
            key = hash(name)
        else:
            normalized_tags = self.__normalize_tags(tags)
            key = hash((name, frozenset(normalized_tags)))

        if key in buffer:
            new_value = buffer[key][1] + value if not replace else value
            buffer[key] = (name, new_value, tags)
        else:
            buffer[key] = (name, value, tags)

    def __normalize_tags(self, tags: Tags) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def increment(
        self,
        name: str,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__add_to_buffer(self.__counters, name, value, tags or {})
        self.__throttled_flush()

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__add_to_buffer(self.__gauges, name, value, tags or {}, replace=True)
        self.__throttled_flush()

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__add_to_buffer(self.__timers, name, value, tags or {})
        self.__throttled_flush()

    def __throttled_flush(self) -> None:
        if time.time() - self.__last_flush_time >= self.__throttle_interval_sec:
            self.flush()

    def flush(self) -> None:
        for name, value, tags in self.__timers.values():
            self.__backend.timing(name, value, tags=tags)
        for name, value, tags in self.__counters.values():
            self.__backend.increment(name, value, tags=tags)
        for name, value, tags in self.__gauges.values():
            self.__backend.gauge(name, value, tags=tags)

        self.__reset()

    def __reset(self) -> None:
        self.__timers.clear()
        self.__counters.clear()
        self.__gauges.clear()
        self.__last_flush_time = time.time()


def _tags_from_mapping(tags: Optional[Mapping[str, str]]) -> Tags:
    if not tags:
        return {}
    return dict(tags)


class ArroyoMetricsBackend:
    """
    Facade that adapts Arroyo metric names and tag mappings to MetricsBackend.
    """

    def __init__(self, backend: MetricsBackend) -> None:
        self.__backend = backend

    def increment(
        self,
        name: ArroyoMetricName,
        value: Union[int, float] = 1,
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__backend.increment(name, value, tags=_tags_from_mapping(tags))

    def gauge(
        self,
        name: ArroyoMetricName,
        value: Union[int, float],
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__backend.gauge(name, value, tags=_tags_from_mapping(tags))

    def timing(
        self,
        name: ArroyoMetricName,
        value: Union[int, float],
        tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__backend.timing(name, value, tags=_tags_from_mapping(tags))


_inner_metrics_backend: Optional[MetricsBackend] = None
_metrics_backend: Optional[MetricsBackend] = None
_dummy_metrics_backend = DummyMetricsBackend()


def configure_metrics(metrics: MetricsBackend, force: bool = False) -> None:
    """
    Metrics can generally only be configured once, unless force is passed
    on subsequent initializations.
    """
    global _metrics_backend
    global _inner_metrics_backend
    if not force:
        assert _metrics_backend is None, "Metrics is already set"

    # Perform a runtime check of metrics instance upon initialization of
    # this class to avoid errors down the line when it is used.
    assert isinstance(metrics, MetricsBackend)

    _inner_metrics_backend = metrics
    _metrics_backend = BufferedMetricsBackend(metrics, throttle_interval_sec=METRICS_FREQUENCY_SEC)
    arroyo_configure_metrics(ArroyoMetricsBackend(_metrics_backend))


def get_inner_metrics() -> MetricsBackend:
    if _inner_metrics_backend is None:
        return _dummy_metrics_backend
    return _inner_metrics_backend


def get_metrics() -> Metrics:
    if _metrics_backend is None:
        return Metrics(_dummy_metrics_backend)
    return Metrics(_metrics_backend)


def get_size(obj: Any) -> int | None:
    # TODO: Make this work for all types
    if isinstance(obj, (str, bytes)):
        return len(obj)
    return None
