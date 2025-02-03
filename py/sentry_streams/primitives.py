from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping

from sentry_streams.adapters.stream_adapter import StreamAdapter


class RuntimeTranslator:
    def __init__(self, runtime_adapter: StreamAdapter):
        self.adapter = runtime_adapter

    def translate_source(self, step_config: Mapping[str, Any]) -> Any:
        translated_fn = getattr(self.adapter, "source")

        return translated_fn(step_config)

    def translate_with_input(
        self, step: WithInput, step_config: Mapping[str, Any], stream: Any
    ) -> Any:
        next_step = step.step_type
        translated_fn = getattr(self.adapter, next_step)

        return translated_fn(step_config, stream)


class PipelineGraph:
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


class Pipeline:
    def __init__(self) -> None:
        self.graph = PipelineGraph()

    def set_translator(self, translator: RuntimeTranslator) -> None:
        self.translator = translator


@dataclass
class _Stage:
    name: str
    ctx: Pipeline


class Step(_Stage):
    step_type: str

    def __post_init__(self) -> None:
        self.ctx.graph.register(self)


class Source(Step):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.ctx.graph.register_source(self)

    def apply_source(self) -> Any: ...


@dataclass
class WithInput(Step):
    inputs: list[Step]

    def __post_init__(self) -> None:
        super().__post_init__()
        for input in self.inputs:
            self.ctx.graph.register_edge(input, self)

    def apply_edge(self, stream: Any) -> Any: ...


@dataclass
class RawKafkaSink(WithInput):
    logical_topic: str
    step_type: str

    def apply_edge(self, stream: Any) -> Any:
        step_config = {"topic": self.logical_topic}
        sink_output = self.ctx.translator.translate_with_input(self, step_config, stream)

        return sink_output


@dataclass
class RawKafkaSource(Source):
    logical_topic: str
    step_type: str

    def apply_source(self) -> Any:
        step_config = {"topic": self.logical_topic}
        stream = self.ctx.translator.translate_source(step_config)

        return stream


@dataclass
class Printer(WithInput):
    def apply_edge(self, stream: Any) -> Any:
        return stream.print()
