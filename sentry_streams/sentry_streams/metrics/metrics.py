from __future__ import annotations

import logging
import time
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Literal,
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

MetricsBackendKind = Literal["dummy", "datadog", "log"]


@dataclass(frozen=True, slots=True)
class StreamMetricsConfig:
    """
    Picklable metrics settings for the streaming process and multiprocessing workers.

    ``throttle_interval_sec`` is the flush interval for :class:`BufferedMetricsBackend`,
    which wraps the inner backend configured via ``backend``.
    """

    backend: MetricsBackendKind
    throttle_interval_sec: float = METRICS_FREQUENCY_SEC
    host: Optional[str] = None
    port: Optional[int] = None
    tags: Optional[Tags] = None
    udp_queue_size: Optional[int] = None


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
class MetricsBackend(Protocol):
    """
    Provides an interface to produce metrics of counter, gauge and timing types.

    This can be implemented by different backends, such as Datadog to actually
    produce metrics on a real channel or platform.
    This can be wrapped in an adapter class that provides a client-specific
    metrics interface, for example Arroyo metrics.
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
    Useful for tests.
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
    Backend to produce metrics to Datadog through the DogStatsd client.

    For each metric produced, a call is made to the datadog agent almost
    immediately. Instances of this class can be provided with default tags
    to be attached to each metric and with a prefix to be added to each
    metric name.
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
        # Ignore mypy: this method is untyped but is part of the public API.
        self.datadog_client.enable_background_sender(  # type: ignore[no-untyped-call]
            sender_queue_size=udp_queue_size if udp_queue_size is not None else SENDER_QUEUE_SIZE,
            sender_queue_timeout=SENDER_QUEUE_TIMEOUT,
        )
        self.__tags: Tags = tags if tags is not None else {}

    def __normalize_tags(self, tags: Tags) -> list[str]:
        return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def __datadog_tags_kw(self, tags: Optional[Tags]) -> Optional[list[str]]:
        combined = _combine_tags(self.__tags, tags)
        normalized = self.__normalize_tags(combined)
        return normalized if normalized else None

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.datadog_client.increment(name, value, tags=self.__datadog_tags_kw(tags))

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.datadog_client.gauge(name, value, tags=self.__datadog_tags_kw(tags))

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.datadog_client.timing(name, value, tags=self.__datadog_tags_kw(tags))


class LogMetricsBackend(MetricsBackend):
    """
    Metrics backend that logs each update immediately, using the same segment format
    as LogFlusher: ``prefix | counter|gauge|timing name=value tag1:val1 ...``.
    """

    def __init__(self, tags: Optional[Tags] = None) -> None:
        self.__prefix = METRICS_PREFIX.strip(".")
        self.__base_tags: Tags = tags if tags is not None else {}

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
    This delegate class wraps a MetricsBackend and buffers metrics to flush them
    periodically.

    This kind of pattern is especially useful when we produce metrics at
    high throughput or in a tight loop so we do not incur the overhead
    of producing metrics on each call.

    An alternative option would be to use Datadog metrics sampling, but that
    would only work on the Datadog backend. Moreover this backend aggregates
    the metric to be produced rather than sampling, so we preserve metrics
    that are produced rarely.
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


class Metrics:
    """
    An adapter to a Metrics backend for the Sentry Streams application.
    The only added value to the metrics backend is that the metric name has
    to be defined in the enum.
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


class ArroyoMetricsBackend:
    """
    An adapter to the Metrics backend used in the Arroyo library. Arroyo allows
    the client application to provide its own metrics implementation. The
    implementation has to comply with arroyo.utils.metrics.Metrics.
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
_streams_metrics_config: Optional[StreamMetricsConfig] = None
_dummy_metrics_backend = DummyMetricsBackend()


def build_metrics_backend(config: StreamMetricsConfig) -> MetricsBackend:
    """Construct the inner (unbuffered) metrics backend from picklable settings."""
    if config.backend == "dummy":
        return DummyMetricsBackend()
    if config.backend == "datadog":
        if config.host is None or config.port is None:
            raise ValueError("datadog metrics require host and port")
        tag_values = dict(config.tags) if config.tags else None
        return DatadogMetricsBackend(
            config.host,
            config.port,
            tags=tag_values,
            udp_queue_size=config.udp_queue_size,
        )
    if config.backend == "log":
        tag_values = dict(config.tags) if config.tags else None
        return LogMetricsBackend(tags=tag_values)
    raise ValueError(f"Unknown metrics backend: {config.backend!r}")


def configure_metrics(config: StreamMetricsConfig, force: bool = False) -> None:
    """
    Metrics can generally only be configured once, unless force is passed
    on subsequent initializations.

    This method has to be called for each process the application uses.
    Accepts a picklable :class:`StreamMetricsConfig` so worker processes can
    rebuild the same backends under ``spawn`` multiprocessing.
    """
    global _metrics_backend
    global _inner_metrics_backend
    global _streams_metrics_config
    if not force:
        assert _metrics_backend is None, "Metrics is already set"

    inner = build_metrics_backend(config)
    _streams_metrics_config = config
    _inner_metrics_backend = inner
    _metrics_backend = BufferedMetricsBackend(
        inner,
        throttle_interval_sec=config.throttle_interval_sec,
    )
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
