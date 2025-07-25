from __future__ import annotations

from abc import abstractmethod
from typing import Mapping, Optional, Protocol, Union, runtime_checkable

from datadog import DogStatsd  # type: ignore[import-not-found]

Tags = Mapping[str, str]


@runtime_checkable
class Metrics(Protocol):
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


class DummyMetricsBackend(Metrics):
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


class DatadogMetricsBackend(Metrics):
    """
    Datadog metrics backend.
    """

    def __init__(self, host: str, port: int, prefix: str, tags: Optional[Tags] = None) -> None:
        self.__datadog_client = DogStatsd(
            DogStatsd,
            host=host,
            port=port,
            namespace=prefix,
            constant_tags=(
                [f"{key}:{value}" for key, value in tags.items()] if tags is not None else None
            ),
        )

    def increment(
        self,
        name: str,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__datadog_client.increment(name, value, tags)

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__datadog_client.gauge(name, value, tags)

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__datadog_client.timing(name, value, tags)


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


def get_metrics() -> Metrics:
    if _metrics_backend is None:
        return _dummy_metrics_backend
    return _metrics_backend
