from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    TypeVar,
    Union,
    cast,
)

from sentry_streams.modules import get_module
from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.window import MeasurementUnit, Window


class StepType(Enum):
    BRANCH = "branch"
    FILTER = "filter"
    MAP = "map"
    REDUCE = "reduce"
    ROUTER = "router"
    SINK = "sink"
    SOURCE = "source"


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


RoutingFuncReturnType = TypeVar("RoutingFuncReturnType")
TransformFuncReturnType = TypeVar("TransformFuncReturnType")


@dataclass
class TransformStep(WithInput, Generic[TransformFuncReturnType]):
    """
    A generic step representing a step performing a transform operation
    on input data.
    function: supports reference to a function using dot notation, or a Callable
    """

    function: Union[Callable[..., TransformFuncReturnType], str]
    step_type: StepType

    @property
    def resolved_function(self) -> Callable[..., TransformFuncReturnType]:
        """
        Returns a callable of the transform function defined, or referenced in the
        this class
        """
        if callable(self.function):
            return self.function

        fn_path = self.function
        mod, cls, fn = fn_path.rsplit(".", 2)

        module = get_module(mod)

        imported_cls = getattr(module, cls)
        imported_func = cast(Callable[..., TransformFuncReturnType], getattr(imported_cls, fn))
        function_callable = imported_func
        return function_callable


@dataclass
class Map(TransformStep[Any]):
    """
    A simple 1:1 Map, taking a single input to single output.
    """

    # We support both referencing map function via a direct reference
    # to the symbol and through a string.
    # The direct reference to the symbol allows for strict type checking
    # The string is likely to be used in cross code base pipelines where
    # the symbol is just not present in the current code base.
    step_type: StepType = StepType.MAP

    # TODO: Allow product to both enable and access
    # configuration (e.g. a DB that is used as part of Map)


@dataclass
class Filter(TransformStep[bool]):
    """
    A simple Filter, taking a single input and either returning it or None as output.
    """

    step_type: StepType = StepType.FILTER


@dataclass
class Branch(Step):
    """
    A Branch represents one branch in a pipeline, which is routed to
    by a Router.
    """

    step_type: StepType = StepType.BRANCH


@dataclass
class Router(WithInput, Generic[RoutingFuncReturnType]):
    """
    A step which takes a routing table of Branches and sends messages
    to those branches based on a routing function.
    Routing functions must only return a single output branch, routing
    to multiple branches simultaneously is not currently supported.
    """

    routing_function: Callable[..., RoutingFuncReturnType]
    routing_table: Mapping[RoutingFuncReturnType, Branch]
    step_type: StepType = StepType.ROUTER

    def __post_init__(self) -> None:
        super().__post_init__()
        for branch_step in self.routing_table.values():
            self.ctx.register_edge(self, branch_step)


@dataclass
class Reduce(WithInput, Generic[MeasurementUnit, InputType, OutputType]):
    """
    A Reduce step which performs windowed aggregations. Can be keyed or non-keyed on the
    input stream. Supports an Accumulator-style aggregation which can have a configurable
    storage backend, for flushing intermediate aggregates.
    """

    windowing: Window[MeasurementUnit]
    aggregate_fn: Callable[[], Accumulator[InputType, OutputType]]
    aggregate_backend: Optional[AggregationBackend[OutputType]] = None
    group_by_key: Optional[GroupBy] = None
    step_type: StepType = StepType.REDUCE
