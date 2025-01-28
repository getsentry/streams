from __future__ import annotations
from dataclasses import dataclass
from typing import Any

class Pipeline:
    def __init__(self) -> None:
        self.steps: dict[str, Step] = {}
        self.edges: dict[str, list[str]] = {}
        self.sources: list[Source] = []

    def register(self, step: Step) -> None:
        assert step.name not in self.steps
        self.steps[step.name] = step

    def register_edge(self, _from: Step , _to: Step ) -> None:
        self.edges.setdefault(_from.name, []).append(_to.name)

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

    def apply_source(self, env: Any, environment_config: dict[str, Any]) -> Any:
        ...

@dataclass
class WithInput(Step):
    inputs: list[Step]

    def __post_init__(self) -> None:
        super().__post_init__()
        for input in self.inputs:
            self.ctx.register_edge(input, self)

    def apply_edge(self, stream: Any, environment_config: dict[str, Any]) -> Any:
        ...


@dataclass
class RawKafkaSource(Source):
    logical_topic: str

    def apply_source(self, env: Any, environment_config: dict[str, Any]) -> Any:
        # TODO: split this out into a completely separate file?
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors import FlinkKafkaConsumer  # type: ignore
        KAFKA_BROKER = "localhost:9092"
        kafka_consumer = FlinkKafkaConsumer(
            topics=environment_config['topics'][self.logical_topic],
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
