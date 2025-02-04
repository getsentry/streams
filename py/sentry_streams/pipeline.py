from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sentry_streams.adapters.stream_adapter import StreamAdapter


class RuntimeTranslator:
    def __init__(self, runtime_adapter: StreamAdapter):
        self.adapter = runtime_adapter

    def translate_source(self, source: Source) -> Any:
        translated_fn = getattr(self.adapter, "source")

        step_config = {}

        if hasattr(source, "logical_topic"):
            step_config["topic"] = source.logical_topic

        return translated_fn(step_config)

    def translate_with_input(self, step: WithInput, stream: Any) -> Any:
        next_step = step.step_type
        translated_fn = getattr(self.adapter, next_step)
        step_config = {}

        if hasattr(step, "logical_topic"):
            step_config["topic"] = step.logical_topic

        return translated_fn(step_config, stream)


# Iterator has all the info about each node that it
# needs, you can just access the canonical primitive name
# and then build up the stream

# if you had a Visitor, visiting each node means
# you can abstract away children
# Visitor would manage the env/stream
# each node would have a reference to its children nodes
# Visitor doesn't have access to any private attributes of nodes


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
    step_type: str


class Step(_Stage):
    def __post_init__(self) -> None:
        self.ctx.register(self)


class Source(Step):
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
class RawKafkaSink(WithInput):
    logical_topic: str


@dataclass
class RawKafkaSource(Source):
    logical_topic: str
