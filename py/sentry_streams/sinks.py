from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any


class Pipeline:
    def __init__(self) -> None:
        self.steps: dict[str, Step] = {}
        self.incoming_edges: dict[str, list[str]] = defaultdict(list)
        self.outgoing_edges: dict[str, list[str]] = defaultdict(list)
        self.sources: list[Source] = []

    def register(self, step: Step) -> None:
        assert step.name not in self.steps
        self.steps[step.name] = step

    def register_edge(self, _from: Step, _to: Step) -> None:
        self.incoming_edges[_to.name].append(_from.name)
        self.outgoing_edges[_from.name].append(_to.name)

    def register_source(self, step: Source) -> None:
        self.sources.append(step)


@dataclass
class _Stage:
    name: str
    ctx: Pipeline


class Step(_Stage):
    def __post_init__(self) -> None:
        self.ctx.register(self)


class Source(Step):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.ctx.register_source(self)

    def apply_source(self, env: Any, environment_config: dict[str, Any]) -> Any: ...


@dataclass
class WithInput(Step):
    inputs: list[Step]

    def __post_init__(self) -> None:
        super().__post_init__()
        for input in self.inputs:
            self.ctx.register_edge(input, self)

    def apply_edge(self, stream: Any, environment_config: dict[str, Any]) -> Any: ...


# Currently, API primitives are coupled
# with their translations to the runtime
# For example, below is highly Flink-specific
@dataclass
class RawKafkaSink(WithInput):
    logical_topic: str

    def apply_edge(self, stream: Any, environment_config: dict[str, Any]) -> Any:
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

        KAFKA_BROKER = "localhost:9092"

        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BROKER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(
                    environment_config["topics"][self.logical_topic],
                )
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        return stream.sink_to(sink)


@dataclass
class RawKafkaSource(Source):
    logical_topic: str

    def apply_source(self, env: Any, environment_config: dict[str, Any]) -> Any:
        # TODO: split this out into a completely separate file?
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors import (  # type: ignore[no-untyped-defs]
            FlinkKafkaConsumer,
        )

        KAFKA_BROKER = "localhost:9092"
        kafka_consumer = FlinkKafkaConsumer(
            topics=environment_config["topics"][self.logical_topic],
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": "python-flink-consumer",
            },
        )

        kafka_consumer.set_start_from_earliest()
        return env.add_source(kafka_consumer)


@dataclass
class Printer(WithInput):
    def apply_edge(self, stream: Any, environment_config: dict[str, Any]) -> Any:
        return stream.print()
