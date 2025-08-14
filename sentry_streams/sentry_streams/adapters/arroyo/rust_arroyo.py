from __future__ import annotations

import logging
import time
from dataclasses import replace
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Self,
    Sequence,
    Type,
    cast,
)

from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
)

from sentry_streams.adapters.arroyo.multi_process_delegate import (
    MultiprocessDelegateFactory,
)
from sentry_streams.adapters.arroyo.reduce import MetricsReportingReduce
from sentry_streams.adapters.arroyo.reduce_delegate import ReduceDelegateFactory
from sentry_streams.adapters.arroyo.routers import build_branches
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps_chain import TransformChains
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.config_types import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
    MultiProcessConfig,
    StepConfig,
)
from sentry_streams.metrics import Metric, get_metrics, get_size
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    ComplexStep,
    Filter,
    FlatMap,
    GCSSink,
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
from sentry_streams.rust_streams import (
    ArroyoConsumer,
    InitialOffset,
    PyKafkaConsumerConfig,
    PyKafkaProducerConfig,
    PyMetricConfig,
)
from sentry_streams.rust_streams import Route as RustRoute
from sentry_streams.rust_streams import (
    RuntimeOperator,
)

logger = logging.getLogger(__name__)


def input_metrics(name: str, message_size: int | None) -> float:
    metrics = get_metrics()
    tags = {"step": name}
    metrics.increment(Metric.INPUT_MESSAGES, tags=tags)
    if message_size is not None:
        metrics.increment(Metric.INPUT_BYTES, tags=tags, value=message_size)
    return time.time()


def output_metrics(
    name: str, error: str | None, start_time: float, message_size: int | None
) -> None:
    metrics = get_metrics()
    tags = {"step": name}
    if error:
        tags["error"] = error
        metrics.increment(Metric.ERRORS, tags=tags)

    metrics.increment(Metric.OUTPUT_MESSAGES, tags=tags)
    if message_size is not None:
        metrics.increment(Metric.OUTPUT_BYTES, tags=tags, value=message_size)
    metrics.timing(Metric.DURATION, time.time() - start_time, tags=tags)


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


def build_kafka_consumer_config(source: str, source_config: StepConfig) -> PyKafkaConsumerConfig:
    """
    Build the Kafka consumer configuration for the source.
    """
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
    assert sink_config is not None, f"Config not provided for StreamSink {sink}"

    producer_config = cast(KafkaProducerConfig, sink_config)
    return PyKafkaProducerConfig(
        bootstrap_servers=producer_config["bootstrap_servers"],
        override_params=cast(Mapping[str, str], producer_config.get("override_params", {})),
    )


def finalize_chain(chains: TransformChains, route: Route) -> RuntimeOperator:
    rust_route = RustRoute(route.source, route.waypoints)
    config, func = chains.finalize(route)
    if config:
        return RuntimeOperator.PythonAdapter(
            rust_route,
            MultiprocessDelegateFactory(
                func,
                config["batch_size"],
                config["batch_time"],
                MultiprocessingPool(
                    num_processes=config["processes"],
                ),
                input_block_size=config.get("input_block_size"),
                output_block_size=config.get("output_block_size"),
                max_input_block_size=config.get("max_input_block_size"),
                max_output_block_size=config.get("max_output_block_size"),
            ),
        )
    else:
        return RuntimeOperator.Map(rust_route, lambda msg: func(msg).to_inner())


class RustArroyoAdapter(StreamAdapter[Route, Route]):
    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        metric_config: PyMetricConfig | None = None,
    ) -> None:
        super().__init__()
        self.steps_config = steps_config
        self.__metric_config = metric_config
        self.__consumers: MutableMapping[str, ArroyoConsumer] = {}
        self.__chains = TransformChains()

    @classmethod
    def build(
        cls,
        config: PipelineConfig,
        metric_config: PyMetricConfig | None = None,
    ) -> Self:
        steps_config = config["steps_config"]

        return cls(steps_config, metric_config)

    def __close_chain(self, stream: Route) -> None:
        if self.__chains.exists(stream):
            logger.info(f"Closing transformation chain: {stream} and adding to pipeline")
            self.__consumers[stream.source].add_step(finalize_chain(self.__chains, stream))

    def get_consumer(self, source: str) -> ArroyoConsumer:
        return self.__consumers[source]

    def complex_step_override(
        self,
    ) -> dict[Type[ComplexStep[Any, Any]], Callable[[ComplexStep[Any, Any]], Route]]:
        return {}

    def source(self, step: Source[Any]) -> Route:
        """
        Builds an Arroyo Kafka consumer as a stream source.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        assert isinstance(step, StreamSource)
        source_name = step.name
        source_config = self.steps_config.get(source_name)
        assert source_config is not None, f"Config not provided for source {source_name}"

        self.__consumers[source_name] = ArroyoConsumer(
            source=source_name,
            kafka_config=build_kafka_consumer_config(source_name, source_config),
            topic=step.stream_name,
            schema=step.stream_name,
            metric_config=self.__metric_config,
        )
        return Route(source_name, [])

    def sink(self, step: Sink[Any], stream: Route) -> Route:
        """
        Builds an Arroyo Kafka producer as a stream sink.
        By default it uses the configuration provided to the adapter.

        It is possible to override the configuration by providing an
        instantiated consumer for unit testing purposes.
        """
        route = RustRoute(stream.source, stream.waypoints)
        self.__close_chain(stream)

        if isinstance(step, GCSSink):
            if sink_config := self.steps_config.get(step.name):
                bucket = (
                    step.bucket if not sink_config.get("bucket") else str(sink_config.get("bucket"))
                )
                parallelism_config = cast(Mapping[str, Any], sink_config.get("parallelism"))
                if parallelism_config:
                    thread_count = cast(int, parallelism_config["threads"])
                else:
                    thread_count = 1
            else:
                bucket = step.bucket
                thread_count = 1

            # TODO: This object_generator is just used to get the name of the object that is being written.
            # Fix this to wrap the actual step instead of just the object_generator.
            # This will at least capture the number of calls to the step, if nothing else.
            def wrapped_generator() -> str:
                start_time = input_metrics(step.name, None)
                try:
                    return step.object_generator()
                finally:
                    output_metrics(step.name, None, start_time, None)

            logger.info(f"Adding GCS sink: {step.name} to pipeline")
            self.__consumers[stream.source].add_step(
                RuntimeOperator.GCSSink(route, bucket, wrapped_generator, thread_count)
            )

        # Our fallback for now since there's no other Sink type
        else:
            assert isinstance(step, StreamSink)
            logger.info(f"Adding stream sink: {step.name} to pipeline")
            self.__consumers[stream.source].add_step(
                RuntimeOperator.StreamSink(
                    route,
                    step.stream_name,
                    build_kafka_producer_config(step.name, self.steps_config),
                )
            )

        return stream

    def map(self, step: Map[Any, Any], stream: Route) -> Route:
        """
        Builds a map operator for the platform the adapter supports.
        """
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a map"

        step_config: Mapping[str, Any] = self.steps_config.get(step.name, {})
        parallelism_config = step_config.get("parallelism")
        application_function = step.resolved_function

        def wrapped_function(msg: Message[Any]) -> Any:
            msg_size = get_size(msg.payload) if hasattr(msg, "payload") else None
            start_time = input_metrics(step.name, msg_size)
            has_error = output_size = None
            try:
                result = application_function(msg)
                output_size = get_size(result)
                return result
            except Exception as e:
                has_error = str(e.__class__.__name__)
                raise e
            finally:
                output_metrics(step.name, has_error, start_time, output_size)

        step = replace(step, function=wrapped_function)

        if step_config.get("starts_segment") or not self.__chains.exists(stream):
            logger.info(f"Starting new segment at step {step.name}")
            if parallelism_config:
                multi_process_config = cast(MultiProcessConfig, parallelism_config["multi_process"])
            else:
                multi_process_config = None

            if self.__chains.exists(stream):
                self.__close_chain(stream)

            self.__chains.init_chain(stream, multi_process_config)
            self.__chains.add_map(stream, step)

        else:
            assert not parallelism_config, "Parallelism config can only be set on a new segment"
            if self.__chains.exists(stream):
                self.__chains.add_map(stream, step)

        return stream

    def flat_map(self, step: FlatMap[Any, Any], stream: Route) -> Route:
        """
        Builds a flat-map operator for the platform the adapter supports.
        """
        logger.info(f"Adding flatMap: {step.name} to pipeline")
        raise NotImplementedError

    def filter(self, step: Filter[Any], stream: Route) -> Route:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        self.__close_chain(stream)
        assert (
            stream.source in self.__consumers
        ), f"Stream starting at source {stream.source} not found when adding a map"

        def filter_msg(msg: Message[Any]) -> bool:
            msg_size = get_size(msg.payload) if hasattr(msg, "payload") else None
            start_time = input_metrics(step.name, msg_size)
            has_error = output_size = None
            try:
                result = step.resolved_function(msg)
                output_size = get_size(result)
                return result
            except Exception as e:
                has_error = str(e.__class__.__name__)
                raise e
            finally:
                output_metrics(step.name, has_error, start_time, output_size)

        route = RustRoute(stream.source, stream.waypoints)
        logger.info(f"Adding filter: {step.name} to pipeline")

        self.__consumers[stream.source].add_step(RuntimeOperator.Filter(route, filter_msg))

        return stream

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: Route,
    ) -> Route:
        """
        Build a reduce operator for the platform the adapter supports.
        """
        self.__close_chain(stream)
        route = RustRoute(stream.source, stream.waypoints)
        name = step.name
        loaded_config: Mapping[str, Any] = self.steps_config.get(name, {})
        step.override_config(loaded_config)
        step = MetricsReportingReduce(step, name)

        logger.info(f"Adding reduce: {step.name} to pipeline")

        self.__consumers[stream.source].add_step(
            RuntimeOperator.PythonAdapter(route, ReduceDelegateFactory(step))
        )
        return stream

    def broadcast(
        self,
        step: Broadcast[Any],
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a broadcast operator for the platform the adapter supports.
        """
        self.__close_chain(stream)
        route = RustRoute(stream.source, stream.waypoints)
        logger.info(f"Adding broadcast: {step.name} to pipeline")
        self.__consumers[stream.source].add_step(
            RuntimeOperator.Broadcast(
                route, downstream_routes=[branch.root.name for branch in step.routes]
            )
        )
        return build_branches(stream, step.routes)

    def router(
        self,
        step: Router[RoutingFuncReturnType, Any],
        stream: Route,
    ) -> Mapping[str, Route]:
        """
        Build a router operator for the platform the adapter supports.
        """
        self.__close_chain(stream)
        route = RustRoute(stream.source, stream.waypoints)

        def routing_function(msg: Message[Any]) -> str:
            msg_size = get_size(msg.payload) if hasattr(msg, "payload") else None
            start_time = input_metrics(step.name, msg_size)
            has_error = None
            try:
                waypoint = step.routing_function(msg)
                branch = step.routing_table[waypoint]
                return branch.root.name
            except Exception as e:
                has_error = str(e.__class__.__name__)
                raise e
            finally:
                output_metrics(step.name, has_error, start_time, None)

        logger.info(f"Adding router: {step.name} to pipeline")
        self.__consumers[stream.source].add_step(
            RuntimeOperator.Router(
                route, routing_function, cast(Sequence[str], step.routing_table.values())
            )
        )
        return build_branches(stream, step.routing_table.values())

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
