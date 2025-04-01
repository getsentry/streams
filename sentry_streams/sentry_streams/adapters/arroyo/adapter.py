from __future__ import annotations

from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Self,
    TypedDict,
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
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import (
    FilterStep,
    MapStep,
    RouterStep,
    StreamSinkStep,
)
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
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


class KafkaConsumerConfig(TypedDict):
    bootstrap_servers: str
    auto_offset_reset: str
    consumer_group: str
    additional_settings: Mapping[str, Any]


class KafkaProducerConfig(TypedDict):
    bootstrap_servers: str
    additional_settings: Mapping[str, Any]


class StreamSources:
    def __init__(
        self,
        sources_config: Mapping[str, KafkaConsumerConfig],
        sources_override: Mapping[str, KafkaConsumer] = {},
    ) -> None:
        super().__init__()

        self.__sources_config = sources_config

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
            config = self.__sources_config.get(source_name)
            assert config, f"Config not provided for source {source_name}"
            self.__sources[source_name] = KafkaConsumer(
                build_kafka_consumer_configuration(
                    default_config=config["additional_settings"],
                    bootstrap_servers=config["bootstrap_servers"],
                    auto_offset_reset=config["auto_offset_reset"],
                    group_id=config["consumer_group"],
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
        sources_config: Mapping[str, KafkaConsumerConfig],
        sinks_config: Mapping[str, KafkaProducerConfig],
        sources_override: Mapping[str, KafkaConsumer] = {},
        sinks_override: Mapping[str, KafkaProducer] = {},
    ) -> None:
        super().__init__()
        self.__sources = StreamSources(sources_config, sources_override)
        self.__sinks_config = sinks_config

        # Overrides are for unit testing purposes
        self.__sinks: MutableMapping[str, Any] = {**sinks_override}

        self.__consumers: MutableMapping[str, ArroyoConsumer] = {}
        self.__processors: Mapping[str, StreamProcessor[KafkaPayload]] = {}

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls(
            config["sources_config"],
            config["sinks_config"],
            config.get("sources_override", {}),
            config.get("sinks_override", {}),
        )

    def source(self, step: Source) -> Route:
        """
        Builds an Arroyo Kafka consumer as a stream source.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        source_name = step.name
        self.__sources.add_source(step)
        self.__consumers[source_name] = ArroyoConsumer(source_name)

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
            config = self.__sinks_config.get(sink_name)
            assert config, f"Config not provided for sink {sink_name}"
            producer = KafkaProducer(
                build_kafka_configuration(
                    default_config=config["additional_settings"],
                    bootstrap_servers=config["bootstrap_servers"],
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
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a router"
        self.__consumers[stream.source].add_step(RouterStep(route=stream, pipeline_step=step))

        routes_map: MutableMapping[str, Route] = {}
        for branch in step.routing_table.values():
            branch_waypoints = cast(List[str], stream.waypoints) + [branch.name]
            branch_stream = Route(
                source=stream.source, waypoints=cast(MutableSequence[str], branch_waypoints)
            )
            routes_map[branch.name] = branch_stream

        return routes_map

    def get_processor(self, source: str) -> StreamProcessor[KafkaPayload]:
        """
        Returns the stream processor for the given source.
        """
        return self.__processors[source]

    def create_processors(self) -> None:
        self.__processors = {
            source: StreamProcessor(
                consumer=self.__sources.get_consumer(source),
                topic=self.__sources.get_topic(source),
                processor_factory=ArroyoStreamingFactory(consumer),
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
