from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, MutableMapping

from sentry_streams.user_functions.agg_template import Accumulator


class StepType(Enum):
    SINK = "sink"
    SOURCE = "source"
    MAP = "map"
    REDUCE = "reduce"


class StateBackend(Enum):
    HASH_MAP = "hash_map"


class Window(Enum):
    SLIDING = "sliding"
    TUMBLING = "tumbling"


class Pipeline:
    """
    A graph representing the connections between
    logical Steps.
    """

    def __init__(self) -> None:
        self.steps: MutableMapping[str, Step] = {}
        self.incoming_edges: MutableMapping[str, list[str]] = defaultdict(list)
        self.outgoing_edges: MutableMapping[str, list[str]] = defaultdict(list)
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
class Step:
    """
    A generic Step, whose incoming
    and outgoing edges are registered
    against a Pipeline.
    """

    name: str
    ctx: Pipeline

    def __post_init__(self) -> None:
        self.ctx.register(self)


@dataclass
class Source(Step):
    """
    A generic Source.
    """


@dataclass
class KafkaSource(Source):
    """
    A Source which reads from Kafka.
    """

    logical_topic: str
    step_type: StepType = StepType.SOURCE

    def __post_init__(self) -> None:
        super().__post_init__()
        self.ctx.register_source(self)


@dataclass
class WithInput(Step):
    """
    A generic Step representing a logical
    step which has inputs.
    """

    inputs: list[Step]

    def __post_init__(self) -> None:
        super().__post_init__()
        for input in self.inputs:
            self.ctx.register_edge(input, self)


@dataclass
class Sink(WithInput):
    """
    A generic Sink.
    """


@dataclass
class KafkaSink(Sink):
    """
    A Sink which specifically writes to Kafka.
    """

    logical_topic: str
    step_type: StepType = StepType.SINK


@dataclass
class Map(WithInput):
    """
    A simple 1:1 Map, taking a single input to single output
    """

    # TODO: Support a reference to a function (Callable)
    # instead of a raw string
    # TODO: Allow product to both enable and access
    # configuration (e.g. a DB that is used as part of Map)
    function: Callable[..., Any]
    step_type: StepType = StepType.MAP


@dataclass
class Reduce(WithInput):
    # group_by_key: refactor to Callable reference
    group_by_key: Callable[..., Any]
    # windowing mechanism, is this going to be mandatory?
    # windowing: Window
    # aggregation (use standard accumulator)
    aggregate_fn: Accumulator
    step_type: StepType = StepType.REDUCE
    # storage: a fixed (enum?) set of storage backends we provide
    # consider making this a class
    storage: StateBackend = StateBackend.HASH_MAP

    # keyed stream --> windowed stream --> reduce to datastream
