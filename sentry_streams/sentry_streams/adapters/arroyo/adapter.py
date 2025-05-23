from __future__ import annotations

import logging
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Self,
    cast,
)

from arroyo.backends.kafka.configuration import (
    build_kafka_configuration,
    build_kafka_consumer_configuration,
)
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.processing.processor import StreamProcessor
from arroyo.types import Topic

from sentry_streams.adapters.arroyo.consumer import (
    ArroyoConsumer,
    ArroyoStreamingFactory,
)
from sentry_streams.adapters.arroyo.routers import build_branches
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import (
    BroadcastStep,
    FilterStep,
    MapStep,
    ReduceStep,
    RouterStep,
    StreamSinkStep,
)
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


class StreamSources:
    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        sources_override: Mapping[str, KafkaConsumer] = {},
    ) -> None:
        super().__init__()
        self.config = steps_config

        # Overrides are for unit testing purposes
        self.__source_topics: MutableMapping[str, Topic] = {}
        self.__sources: MutableMapping[str, KafkaConsumer] = {**sources_override}

    def add_source(self, step: Source) -> None:
        """
        Builds an Arroyo Kafka consumer as a stream source.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        # TODO: Provide a better way to get the logical stream name from
        # the Sink step. We should not have to assert it is a Kafka sink
        assert isinstance(step, StreamSource), "Only Stream Sources are supported"
        source_name = step.name

        if source_name not in self.__sources:

            source_config = self.config.get(source_name)
            assert source_config is not None, f"Config not provided for source {source_name}"

            source_config = cast(KafkaConsumerConfig, source_config)

            self.__sources[source_name] = KafkaConsumer(
                build_kafka_consumer_configuration(
                    default_config=source_config.get("additional_settings", {}),
                    bootstrap_servers=source_config.get("bootstrap_servers", ["localhost: 9092"]),
                    auto_offset_reset=(source_config.get("auto_offset_reset", "latest")),
                    group_id=f"pipeline-{source_name}",
                )
            )

        self.__source_topics[source_name] = Topic(step.stream_name)

    def get_topic(self, source: str) -> Topic:
        return self.__source_topics[source]

    def get_consumer(self, source: str) -> KafkaConsumer:
        return self.__sources[source]


class ArroyoAdapter(StreamAdapter[Route, Route]):
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
        steps_config = config["steps_config"]

        return cls(steps_config, sources_override, sinks_override)

    def source(self, step: Source) -> Route:
        """
        Builds an Arroyo Kafka consumer as a stream source.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        source_name = step.name
        self.__sources.add_source(step)

        # This is the Arroyo adapter, and it only supports consuming from StreamSource anyways
        assert isinstance(step, StreamSource)

        self.__consumers[source_name] = ArroyoConsumer(
            source_name, step.stream_name, step.stream_name, step.header_filter
        )

        return Route(source_name, [])

    def sink(self, step: Sink, stream: Route) -> Route:
        """
        Builds an Arroyo Kafka producer as a stream sink.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        # TODO: Provide a better way to get the logical stream name from
        # the Sink step. We should not have to assert it is a Kafka sink
        assert isinstance(step, StreamSink), "Only Stream Sinks are supported"

        sink_name = step.name
        if sink_name not in self.__sinks:

            sink_config = self.steps_config.get(sink_name)
            assert sink_config is not None, f"Config not provided for sink {sink_name}"

            sink_config = cast(KafkaProducerConfig, sink_config)

            producer = KafkaProducer(
                build_kafka_configuration(
                    default_config=sink_config.get("additional_settings", {}),
                    bootstrap_servers=sink_config.get("bootstrap_servers", "localhost:9092"),
                )
            )
        else:
            producer = self.__sinks[sink_name]

        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a producer"

        self.__consumers[stream.source].add_step(
            StreamSinkStep(route=stream, producer=producer, topic_name=step.stream_name)
        )

        return stream

    def map(self, step: Map, stream: Route) -> Route:
        """
        Builds a map operator for the platform the adapter supports.
        """
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a map"

        self.__consumers[stream.source].add_step(MapStep(route=stream, pipeline_step=step))
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
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a filter"

        self.__consumers[stream.source].add_step(FilterStep(route=stream, pipeline_step=step))
        return stream

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: Route,
    ) -> Route:
        """
        Build a reduce operator for the platform the adapter supports.
        """

        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a reduce"

        self.__consumers[stream.source].add_step(ReduceStep(route=stream, pipeline_step=step))
        return stream

    def broadcast(
        self,
        step: Broadcast,
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a broadcast operator for the platform the adapter supports.
        """
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a broadcast step"
        self.__consumers[stream.source].add_step(BroadcastStep(route=stream, pipeline_step=step))

        return build_branches(stream, step.routes)

    def router(
        self,
        step: Router[RoutingFuncReturnType],
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a router operator for the platform the adapter supports.
        """
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a router"
        self.__consumers[stream.source].add_step(RouterStep(route=stream, pipeline_step=step))

        return build_branches(stream, step.routing_table.values())

    def get_processor(self, source: str) -> StreamProcessor[KafkaPayload]:
        """
        Returns the stream processor for the given source
        """
        return self.__processors[source]

    def create_processors(self) -> None:
        self.__processors = {
            source: StreamProcessor(
                consumer=self.__sources.get_consumer(source),
                topic=self.__sources.get_topic(source),
                processor_factory=ArroyoStreamingFactory(consumer),
                join_timeout=0.0,
            )
            for source, consumer in self.__consumers.items()
        }

    def run(self) -> None:
        """
        Starts the pipeline
        """
        # TODO: Support multiple consumers
        self.create_processors()
        assert len(self.__consumers) == 1, "Only one consumer is supported"
        source = next(iter(self.__consumers))

        processor = self.__processors[source]
        processor.run()

    def shutdown(self) -> None:
        """
        Shutdown the arroyo processors allowing them to terminate the inflight
        work.
        """
        assert len(self.__consumers) == 1, "Only one consumer is supported"
        source = next(iter(self.__consumers))
        processor = self.__processors[source]
        processor.signal_shutdown()
