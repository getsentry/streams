from __future__ import annotations

import logging
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Self,
    cast,
)

from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.processing.processor import StreamProcessor
from rust_streams import (
    ArroyoConsumer,
    InitialOffset,
    PyKafkaConsumerConfig,
)
from rust_streams import Route as RustRoute
from rust_streams import (
    RuntimeOperator,
)

from sentry_streams.adapters.arroyo.adapter import StreamSources
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.config_types import (
    KafkaConsumerConfig,
    StepConfig,
)
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    FlatMap,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Source,
    StreamSource,
)
from sentry_streams.pipeline.window import MeasurementUnit

logger = logging.getLogger(__name__)


def build_initial_offset(offset_reset: str) -> InitialOffset:
    """
    Build the initial offset for the Kafka consumer.
    """
    if offset_reset == "earliest":
        return InitialOffset.Earliest
    elif offset_reset == "latest":
        return InitialOffset.Latest
    elif offset_reset == "error":
        return InitialOffset.Error
    else:
        raise ValueError(f"Invalid offset reset value: {offset_reset}")


def build_kafka_consumer_config(
    source: str, steps_config: Mapping[str, StepConfig]
) -> PyKafkaConsumerConfig:
    """
    Build the Kafka consumer configuration for the source.
    """
    source_config = steps_config.get(source)
    assert source_config is not None, f"Config not provided for source {source}"

    consumer_config = cast(KafkaConsumerConfig, source_config)
    bootstrap_servers = consumer_config["bootstrap_servers"]
    group_id = f"pipeline-{source}"
    auto_offset_reset = build_initial_offset(consumer_config.get("auto_offset_reset", "latest"))
    strict_offset_reset = bool(consumer_config.get("strict_offset_reset", False))
    override_params = cast(Mapping[str, str], consumer_config.get("override_params", {}))

    return PyKafkaConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=strict_offset_reset,
        max_poll_interval_ms=60000,
        override_params=override_params,
    )


class RustArroyoAdapter(StreamAdapter[Route, Route]):
    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        sources_override: Mapping[str, KafkaConsumer] = {},
        sinks_override: Mapping[str, KafkaProducer] = {},
    ) -> None:
        super().__init__()
        self.steps_config = steps_config
        self.__sources = StreamSources(steps_config, sources_override)

        # Overrides are for unit testing purposes
        self.__sinks: MutableMapping[str, Any] = {**sinks_override}

        self.__consumers: MutableMapping[str, ArroyoConsumer] = {}
        self.__processors: Mapping[str, StreamProcessor[KafkaPayload]] = {}

    @classmethod
    def build(
        cls,
        config: PipelineConfig,
        sources_override: Mapping[str, KafkaConsumer] = {},
        sinks_override: Mapping[str, KafkaProducer] = {},
    ) -> Self:
        steps_config = config["pipeline"]["segments"][0]["steps_config"]

        return cls(steps_config, sources_override, sinks_override)

    def source(self, step: Source) -> Route:
        """
        Builds an Arroyo Kafka consumer as a stream source.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        assert isinstance(step, StreamSource)
        source_name = step.name
        self.__consumers[source_name] = ArroyoConsumer(
            source=source_name,
            kafka_config=build_kafka_consumer_config(source_name, self.steps_config),
            topic=step.stream_name,
        )

        return Route(source_name, [])

    def sink(self, step: Sink, stream: Route) -> Route:
        """
        Builds an Arroyo Kafka producer as a stream sink.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        raise NotImplementedError

    def map(self, step: Map, stream: Route) -> Route:
        """
        Builds a map operator for the platform the adapter supports.
        """
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a map"

        route = RustRoute(stream.source, stream.waypoints)
        self.__consumers[stream.source].add_step(RuntimeOperator.Map(route, step.resolved_function))
        return stream

    def flat_map(self, step: FlatMap, stream: Route) -> Route:
        """
        Builds a flat-map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def filter(self, step: Filter, stream: Route) -> Route:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: Route,
    ) -> Route:
        """
        Build a reduce operator for the platform the adapter supports.
        """

        raise NotImplementedError

    def broadcast(
        self,
        step: Broadcast,
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a broadcast operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def router(
        self,
        step: Router[RoutingFuncReturnType],
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a router operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def run(self) -> None:
        """
        Starts the pipeline
        """
        # TODO: Support multiple consumers
        assert len(self.__consumers) == 1, "Multiple consumers not supported yet"
        consumer = next(iter(self.__consumers.values()))
        consumer.run()

    def shutdown(self) -> None:
        """
        Shutdown the arroyo processors allowing them to terminate the inflight
        work.
        """
        raise NotImplementedError
