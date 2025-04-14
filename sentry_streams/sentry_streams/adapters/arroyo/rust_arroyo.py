from __future__ import annotations

import logging
from typing import (
    Mapping,
    MutableMapping,
    Self,
    cast,
)

from rust_streams import (
    ArroyoConsumer,
    InitialOffset,
    PyKafkaConsumerConfig,
    PyKafkaProducerConfig,
)
from rust_streams import Route as RustRoute
from rust_streams import (
    RuntimeOperator,
)

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.config_types import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
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
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import MeasurementUnit

logger = logging.getLogger(__name__)


def build_initial_offset(offset_reset: str) -> InitialOffset:
    """
    Build the initial offset for the Kafka consumer.
    """
    if offset_reset == "earliest":
        return InitialOffset.earliest
    elif offset_reset == "latest":
        return InitialOffset.latest
    elif offset_reset == "error":
        return InitialOffset.error
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


def build_kafka_producer_config(
    sink: str, steps_config: Mapping[str, StepConfig]
) -> PyKafkaProducerConfig:
    sink_config = steps_config.get(sink)
    assert sink_config is not None, f"Config not provided for source {sink}"

    producer_config = cast(KafkaProducerConfig, sink_config)
    return PyKafkaProducerConfig(
        bootstrap_servers=producer_config["bootstrap_servers"],
        override_params=cast(Mapping[str, str], producer_config.get("override_params", {})),
    )


class RustArroyoAdapter(StreamAdapter[Route, Route]):
    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
    ) -> None:
        super().__init__()
        self.steps_config = steps_config
        self.__consumers: MutableMapping[str, ArroyoConsumer] = {}

    @classmethod
    def build(
        cls,
        config: PipelineConfig,
    ) -> Self:
        steps_config = config["pipeline"]["segments"][0]["steps_config"]

        return cls(steps_config)

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
        assert isinstance(step, StreamSink), "Only Stream Sinks are supported"
        route = RustRoute(stream.source, stream.waypoints)

        self.__consumers[stream.source].add_step(
            RuntimeOperator.StreamSink(
                route, step.stream_name, build_kafka_producer_config(step.name, self.steps_config)
            )
        )
        return stream

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
