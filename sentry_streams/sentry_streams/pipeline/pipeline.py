from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    MutableMapping,
    Optional,
    TypeVar,
    Union,
    cast,
)

from sentry_streams.modules import get_module
from sentry_streams.pipeline.batch import BatchBuilder, unbatch
from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.window import MeasurementUnit, TumblingWindow, Window


class StepType(Enum):
    SINK = "sink"
    SOURCE = "source"
    MAP = "map"
    REDUCE = "reduce"
    FILTER = "filter"
    FLAT_MAP = "flat_map"


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


T = TypeVar("T")


class TransformFunction(ABC, Generic[T]):

    @property
    @abstractmethod
    def resolved_function(self) -> Callable[..., T]:
        raise NotImplementedError()


@dataclass
class TransformStep(WithInput, TransformFunction[T]):
    """
    A generic step representing a step performing a transform operation
    on input data.
    function: supports reference to a function using dot notation, or a Callable
    """

    function: Union[Callable[..., T], str]
    step_type: StepType

    @property
    def resolved_function(self) -> Callable[..., T]:
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
        imported_func = cast(Callable[..., T], getattr(imported_cls, fn))
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
class Reduce(WithInput):
    """
    A generic Step for a Reduce (or Accumulator-based) operation
    """

    @property
    @abstractmethod
    def group_by(self) -> Optional[GroupBy]:
        raise NotImplementedError()


@dataclass
class Aggregate(Reduce, Generic[MeasurementUnit, InputType, OutputType]):
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

    @property
    def group_by(self) -> Optional[GroupBy]:
        return self.group_by_key


@dataclass
class Batch(Reduce, Generic[MeasurementUnit, InputType]):
    """
    A step to Batch up the results of the prior step.

    Batch can be configured via batch size, which can be
    an event time duration or a count of events.
    """

    # TODO: Use concept of custom triggers to close window
    # by either size or time
    batch_size: MeasurementUnit
    step_type: StepType = StepType.REDUCE

    def __post_init__(self) -> None:
        super().__post_init__()
        self.windowing: TumblingWindow[MeasurementUnit] = TumblingWindow(self.batch_size)
        self.aggregate_fn = BatchBuilder[InputType]

    @property
    def group_by(self) -> Optional[GroupBy]:
        return None


@dataclass
class FlatMapStep(WithInput):
    """
    A generic step for mapping and flattening (and therefore alerting the shape of) inputs to
    get outputs. Takes a single input to 0...N outputs.
    """

    step_type: StepType = StepType.FLAT_MAP


@dataclass
class FlatMap(FlatMapStep, TransformStep[Any]):
    """
    A FlatMap with a user-defined function.
    """


@dataclass
class Unbatch(FlatMapStep, TransformFunction[T]):
    """
    A step to flatten a batch representation to output its individual elements.
    """

    @property
    def resolved_function(
        self,
    ) -> Callable[..., T]:
        return cast(Callable[..., T], unbatch)
