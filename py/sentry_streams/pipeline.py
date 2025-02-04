from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional

from sentry_streams.adapters.stream_adapter import StreamAdapter


class RuntimeTranslator:
    def __init__(self, runtime_adapter: StreamAdapter):
        self.adapter = runtime_adapter

    def translate_step(self, step: Step, stream: Optional[Any] = None) -> Any:
        assert hasattr(step, "step_type")
        next_step = step.step_type
        translated_fn = getattr(self.adapter, next_step)
        step_config = {}

        # build step-specific config
        if hasattr(step, "logical_topic"):
            step_config["topic"] = step.logical_topic

        if stream:
            return translated_fn(step_config, stream)
        else:
            return translated_fn(step_config)


class Pipeline:
    def __init__(self) -> None:
        self.steps: MutableMapping[str, Step] = {}
        self.incoming_edges: MutableMapping[str, list[str]] = defaultdict(list)
        self.outgoing_edges: MutableMapping[str, list[str]] = defaultdict(list)
        self.sources: list[KafkaSource] = []

    def register(self, step: Step) -> None:
        assert step.name not in self.steps
        self.steps[step.name] = step

    def register_edge(self, _from: Step, _to: Step) -> None:
        self.incoming_edges[_to.name].append(_from.name)
        self.outgoing_edges[_from.name].append(_to.name)

    def register_source(self, step: KafkaSource) -> None:
        self.sources.append(step)


@dataclass
class _Stage:
    name: str
    ctx: Pipeline


@dataclass
class Step(_Stage):
    def __post_init__(self) -> None:
        self.ctx.register(self)


@dataclass
class KafkaSource(Step):
    logical_topic: str
    step_type: str = "source"

    def __post_init__(self) -> None:
        super().__post_init__()
        self.ctx.register_source(self)


@dataclass
class WithInput(Step):
    inputs: list[Step]

    def __post_init__(self) -> None:
        super().__post_init__()
        for input in self.inputs:
            self.ctx.register_edge(input, self)


@dataclass
class KafkaSink(WithInput):
    logical_topic: str
    step_type: str = "sink"
