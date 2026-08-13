"""
Pull-based adapter for the streams pipeline DSL.

Maps pipeline steps to PullOperator variants, which the Rust PullConsumer
converts to concrete pull-based stages. This is a drop-in replacement for
RustArroyoAdapter for pipelines that can run on the pull-based runtime.

Currently supports the items_span steel thread pipeline:
  KafkaSource → HeaderFilter → Batch → PyCallable(parse) → PyCallable(process)
    → PyCallable(serialize) → GcsSink
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Self, Type

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import InputType, OutputType
from sentry_streams.pipeline.pipeline import (
    Batch,
    Broadcast,
    ComplexStep,
    Filter,
    FlatMap,
    GCSSink,
    HeadersFilter,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Source,
    StreamSource,
)
from sentry_streams.pipeline.window import MeasurementUnit
from sentry_streams.rust_streams import (
    PullConsumer,
    PullOperator,
    PullSourceConfig,
    PyKafkaConsumerConfig,
)

logger = logging.getLogger(__name__)


def build_kafka_consumer_config(
    source_name: str,
    source_config: Mapping[str, Any],
    consumer_group: str | None,
) -> PyKafkaConsumerConfig:
    """Build a PyKafkaConsumerConfig from pipeline step config."""
    bootstrap_servers = source_config.get(
        "bootstrap_servers", source_config.get("broker_config", {}).get("bootstrap.servers", "")
    )
    if isinstance(bootstrap_servers, str):
        bootstrap_servers = [bootstrap_servers]

    group_id = consumer_group or source_config.get("consumer_group", f"{source_name}-consumer")
    auto_offset_reset = source_config.get("auto_offset_reset", "earliest")

    # Map string offset reset to the Rust enum
    from sentry_streams.rust_streams import InitialOffset

    offset_map = {
        "earliest": InitialOffset.earliest,
        "latest": InitialOffset.latest,
        "error": InitialOffset.error,
    }
    initial_offset = offset_map.get(auto_offset_reset, InitialOffset.earliest)

    override_params = source_config.get("override_params", None)

    return PyKafkaConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=initial_offset,
        strict_offset_reset=source_config.get("strict_offset_reset", False),
        max_poll_interval_ms=source_config.get("max_poll_interval_ms", 300000),
        override_params=override_params,
    )


class PullBasedAdapter(StreamAdapter[str, str]):
    """
    Pull-based adapter that translates pipeline DSL steps into
    PullOperator variants for the Rust pull-based runtime.

    StreamT = str (just a source name identifier, like Route in push model)
    StreamSinkT = str
    """

    def __init__(
        self,
        steps_config: Mapping[str, Any],
    ) -> None:
        self._steps_config = steps_config
        self._kafka_config: PyKafkaConsumerConfig | None = None
        self._topic: str = ""
        self._schema: str | None = None
        self._steps: list[PullOperator] = []
        self._sink: PullOperator | None = None

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:  # type: ignore[override]
        steps_config = config.get("steps_config", {})
        return cls(steps_config)

    def complex_step_override(
        self,
    ) -> dict[Type[ComplexStep[Any, Any]], Callable[[ComplexStep[Any, Any]], str]]:
        return {}

    def source(self, step: Source[Any]) -> str:
        assert isinstance(step, StreamSource)
        source_name = step.name
        source_config = self._steps_config.get(source_name)
        assert source_config is not None, f"Config not provided for source {source_name}"

        step_config: Mapping[str, Any] = self._steps_config.get(source_name, {})
        step.override_config(step_config)
        step.validate()

        self._kafka_config = build_kafka_consumer_config(
            source_name, source_config, step.consumer_group
        )
        self._topic = step.stream_name
        self._schema = step.stream_name  # schema name matches topic for codec lookup

        return source_name

    def sink(self, step: Sink[Any], stream: str) -> str:
        if isinstance(step, GCSSink):
            self._sink = PullOperator.GcsSink(
                bucket=step.bucket,
                object_generator=step.object_generator,
            )
        else:
            raise NotImplementedError(
                f"PullBasedAdapter does not support sink type: {type(step).__name__}"
            )
        return stream

    def map(self, step: Map[Any, Any], stream: str) -> str:
        assert self._schema is not None, "source() must be called before map()"
        self._steps.append(
            PullOperator.PyCallable(
                callable=step.resolved_function,
                name=step.name,
                schema=self._schema,
            )
        )
        return stream

    def flat_map(self, step: FlatMap[Any, Any], stream: str) -> str:
        raise NotImplementedError("PullBasedAdapter does not support flat_map")

    def filter(self, step: Filter[Any], stream: str) -> str:
        if isinstance(step, HeadersFilter):
            self._steps.append(
                PullOperator.HeaderFilter(
                    header_name=step.header_name,
                    expected_value=step.value,
                )
            )
        else:
            raise NotImplementedError(
                f"PullBasedAdapter does not support filter type: {type(step).__name__}"
            )
        return stream

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: str,
    ) -> str:
        if isinstance(step, Batch):
            assert step.batch_size is not None, "Batch requires batch_size"
            self._steps.append(PullOperator.Batch(max_batch_size=step.batch_size))
        else:
            raise NotImplementedError(
                f"PullBasedAdapter does not support reduce type: {type(step).__name__}"
            )
        return stream

    def router(
        self,
        step: Router[RoutingFuncReturnType, Any],
        stream: str,
    ) -> Mapping[str, str]:
        raise NotImplementedError("PullBasedAdapter does not support router")

    def broadcast(
        self,
        step: Broadcast[Any],
        stream: str,
    ) -> Mapping[str, str]:
        raise NotImplementedError("PullBasedAdapter does not support broadcast")

    def run(self) -> None:
        assert self._kafka_config is not None, "No source configured"

        logger.info("Starting pull-based pipeline with %d steps", len(self._steps))
        source = PullSourceConfig.Kafka(
            config=self._kafka_config,
            topic=self._topic,
        )
        consumer = PullConsumer(
            source=source,
            steps=self._steps,
            sink=self._sink,
        )
        consumer.run()

    def shutdown(self) -> None:
        # TODO: signal the pipeline to stop gracefully
        pass
