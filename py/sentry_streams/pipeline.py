from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import MutableMapping


class Pipeline:
    """
    A graph representing the connections between
    logical Steps.
    """

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
class Sink(WithInput):
    pass


@dataclass
class KafkaSink(Sink):
    logical_topic: str
