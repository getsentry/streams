from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from sentry_streams.modules import get_module
from sentry_streams.pipeline.batch import BatchBuilder
from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_codecs import (
    batch_msg_parser,
    msg_parser,
    msg_serializer,
)
from sentry_streams.pipeline.window import MeasurementUnit, TumblingWindow, Window


class StepType(Enum):
    BRANCH = "branch"
    BROADCAST = "broadcast"
    FILTER = "filter"
    FLAT_MAP = "flat_map"
    MAP = "map"
    REDUCE = "reduce"
    ROUTER = "router"
    SINK = "sink"
    SOURCE = "source"


def make_edge_sets(edge_map: Mapping[str, Sequence[Any]]) -> Mapping[str, Set[Any]]:
    return {k: set(v) for k, v in edge_map.items()}


class Pipeline:
    """
    A graph representing the connections between
    logical Steps.
    """

    def __init__(self, source: Source | Branch) -> None:
        self.steps: MutableMapping[str, Step] = {}
        self.incoming_edges: MutableMapping[str, list[str]] = defaultdict(list)
        self.outgoing_edges: MutableMapping[str, list[str]] = defaultdict(list)

        self.root = source
        self.register(source)
        self.__last_added_step: Step = source
        self._closed = False

    def register(self, step: Step) -> None:
        assert step.name not in self.steps, f"Step {step.name} already exists in the pipeline"
        self.steps[step.name] = step

    def register_edge(self, _from: Step, _to: Step) -> None:
        self.incoming_edges[_to.name].append(_from.name)
        self.outgoing_edges[_from.name].append(_to.name)

    def _merge(self, other: Pipeline, merge_point: str) -> None:
        """
        Merges another pipeline into this one after a provided step identified
        as `merge_point`

        The source of the other pipeline (which must be a Branch) is set to be the child
        of the merge_point step.
        """
        assert not isinstance(
            other.root, Source
        ), "Cannot merge a pipeline into another if it contains a stream source"

        other_pipeline_sources = {
            n for n in other.steps if other.steps[n].name not in other.incoming_edges
        }

        for step in other.steps.values():
            self.register(step)

        for source, dests in other.outgoing_edges.items():
            self.outgoing_edges[source].extend(dests)

        for dest, sources in other.incoming_edges.items():
            for s in sources:
                self.incoming_edges[dest].append(s)

        self.outgoing_edges[merge_point].extend(other_pipeline_sources)
        for n in other_pipeline_sources:
            self.incoming_edges[n].append(merge_point)

    def apply(self, step: Step) -> Pipeline:
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step.register(self, self.__last_added_step)
        self.__last_added_step = step
        return self

    def sink(self, step: Sink) -> Pipeline:
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step.register(self, self.__last_added_step)
        self._closed = True
        return self


def streaming_source(name: str, stream_name: str) -> Pipeline:
    """
    Used to create a new pipeline with a streaming source, where the stream_name is the
    name of the Kafka topic to read from.
    """
    return Pipeline(StreamSource(name=name, stream_name=stream_name))


def branch(name: str) -> Pipeline:
    """
    Used to create a new pipeline with a branch as the root step. This pipeline can then be added as part of
    a router or broadcast step.
    """
    return Pipeline(Branch(name=name))


@dataclass
class Step:
    """
    A generic Step, whose incoming
    and outgoing edges are registered
    against a Pipeline.
    """

    name: str

    def register(self, ctx: Pipeline, previous: Step) -> None:
        ctx.register(self)

    def override_config(self, loaded_config: Mapping[str, Any]) -> None:
        """
        Steps can implement custom overriding logic
        """
        pass


@dataclass
class ComplexStep(Step):
    """
    A wrapper around a simple step that allows for syntactic sugar/more complex steps.
    The convert() function must return a simple step.
    """

    @abstractmethod
    def convert(self) -> Step:
        raise NotImplementedError()


@dataclass
class Source(Step):
    """
    A generic Source.
    """


@dataclass
class StreamSource(Source):
    """
    A Source which reads from Kafka.
    """

    stream_name: str
    header_filter: Optional[Tuple[str, bytes]] = None
    step_type: StepType = StepType.SOURCE

    def register(self, ctx: Pipeline, previous: Step) -> None:
        super().register(ctx, previous)


@dataclass
class WithInput(Step):
    """
    A generic Step representing a logical
    step which has inputs.
    """

    def register(self, ctx: Pipeline, previous: Step) -> None:
        super().register(ctx, previous)
        ctx.register_edge(previous, self)


@dataclass
class Sink(WithInput):
    """
    A generic Sink.
    """


@dataclass
class GCSSink(Sink):
    """
    A Sink which writes to GCS
    """

    bucket: str
    object_generator: Callable[[], str]
    step_type: StepType = StepType.SINK


@dataclass
class StreamSink(Sink):
    """
    A Sink which specifically writes to Kafka.
    """

    stream_name: str
    step_type: StepType = StepType.SINK


RoutingFuncReturnType = TypeVar("RoutingFuncReturnType")
TransformFuncReturnType = TypeVar("TransformFuncReturnType")


class TransformFunction(ABC, Generic[TransformFuncReturnType]):
    @property
    @abstractmethod
    def resolved_function(self) -> Callable[..., TransformFuncReturnType]:
        raise NotImplementedError()

    def has_rust_function(self) -> bool:
        """Check if this function provides a Rust implementation"""
        func = self.resolved_function
        return (
            hasattr(func, "get_rust_function_pointer")
            and hasattr(func, "input_type")
            and hasattr(func, "output_type")
            and hasattr(func, "callback_type")
        )

    def get_rust_callback_info(self) -> Mapping[str, Any]:
        """Get information about the Rust callback function"""
        if not self.has_rust_function():
            raise ValueError("Function does not have Rust implementation")

        func = self.resolved_function
        return {
            "function_ptr": func.get_rust_function_pointer(),  # type: ignore[attr-defined]
            "input_type": func.input_type(),  # type: ignore[attr-defined]
            "output_type": func.output_type(),  # type: ignore[attr-defined]
            "callback_type": func.callback_type(),  # type: ignore[attr-defined]
        }


@dataclass
class TransformStep(WithInput, TransformFunction[TransformFuncReturnType]):
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

    def __post_init__(self) -> None:
        """Validate the function after initialization"""
        if self.has_rust_function():
            self._validate_rust_function_type("map")

    def _validate_rust_function_type(self, expected_callback_type: str) -> None:
        """Validate that the Rust function has the correct type"""
        try:
            info = self.get_rust_callback_info()
            if info["callback_type"] != expected_callback_type:
                raise TypeError(
                    f"Function {self.function} is a {info['callback_type']} function, "
                    f"but expected {expected_callback_type}"
                )
        except Exception as e:
            raise TypeError(f"Invalid Rust function {self.function}: {e}")


@dataclass
class Filter(TransformStep[bool]):
    """
    A simple Filter, taking a single input and either returning it or None as output.
    """

    step_type: StepType = StepType.FILTER

    def __post_init__(self) -> None:
        """Validate the function after initialization"""
        if self.has_rust_function():
            self._validate_rust_function_type("filter")

    def _validate_rust_function_type(self, expected_callback_type: str) -> None:
        """Validate that the Rust function has the correct type"""
        try:
            info = self.get_rust_callback_info()
            if info["callback_type"] != expected_callback_type:
                raise TypeError(
                    f"Function {self.function} is a {info['callback_type']} function, "
                    f"but expected {expected_callback_type}"
                )
            # Additional validation for filter: output type should be bool
            if expected_callback_type == "filter" and info["output_type"] != "bool":
                raise TypeError(
                    f"Filter function {self.function} should return bool, "
                    f"but returns {info['output_type']}"
                )
        except Exception as e:
            raise TypeError(f"Invalid Rust function {self.function}: {e}")


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
    routing_table: Mapping[RoutingFuncReturnType, Pipeline]
    step_type: StepType = StepType.ROUTER

    def register(self, ctx: Pipeline, previous: Step) -> None:
        super().register(ctx, previous)
        for pipeline in self.routing_table.values():
            ctx._merge(other=pipeline, merge_point=self.name)


@dataclass
class Broadcast(WithInput):
    """
    A Broadcast step will forward messages to all downstream branches in a pipeline.
    """

    routes: Sequence[Pipeline]
    step_type: StepType = StepType.BROADCAST

    def register(self, ctx: Pipeline, previous: Step) -> None:
        super().register(ctx, previous)
        for chain in self.routes:
            ctx._merge(other=chain, merge_point=self.name)


@dataclass
class Reduce(WithInput, ABC, Generic[MeasurementUnit, InputType, OutputType]):
    """
    A generic Step for a Reduce (or Accumulator-based) operation
    """

    @property
    @abstractmethod
    def group_by(self) -> Optional[GroupBy]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def windowing(self) -> Window[MeasurementUnit]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def aggregate_fn(self) -> Callable[[], Accumulator[InputType, OutputType]]:
        raise NotImplementedError()


@dataclass
class Aggregate(Reduce[MeasurementUnit, InputType, OutputType]):
    """
    A Reduce step which performs windowed aggregations. Can be keyed or non-keyed on the
    input stream. Supports an Accumulator-style aggregation which can have a configurable
    storage backend, for flushing intermediate aggregates.
    """

    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[InputType, OutputType]]
    aggregate_backend: Optional[AggregationBackend[OutputType]] = None
    group_by_key: Optional[GroupBy] = None
    step_type: StepType = StepType.REDUCE

    @property
    def group_by(self) -> Optional[GroupBy]:
        return self.group_by_key

    @property
    def windowing(self) -> Window[MeasurementUnit]:
        return self.window

    @property
    def aggregate_fn(self) -> Callable[[], Accumulator[InputType, OutputType]]:
        return self.aggregate_func


BatchInput = TypeVar("BatchInput")


@dataclass
class Batch(Reduce[MeasurementUnit, InputType, MutableSequence[Tuple[InputType, Optional[str]]]]):
    """
    A step to Batch up the results of the prior step.

    Batch can be configured via batch size, which can be
    an event time duration or a count of events.
    """

    # TODO: Use concept of custom triggers to close window
    # by either size or time
    batch_size: MeasurementUnit
    step_type: StepType = StepType.REDUCE

    @property
    def group_by(self) -> Optional[GroupBy]:
        return None

    @property
    def windowing(self) -> Window[MeasurementUnit]:
        return TumblingWindow(self.batch_size)

    @property
    def aggregate_fn(self) -> Callable[[], Accumulator[InputType, OutputType]]:
        batch_acc = BatchBuilder[BatchInput]
        return cast(Callable[[], Accumulator[InputType, OutputType]], batch_acc)

    def override_config(self, loaded_config: Mapping[str, Any]) -> None:
        merged_config = (
            loaded_config.get("batch_size")
            if loaded_config.get("batch_size") is not None
            else self.batch_size
        )
        self.batch_size = cast(MeasurementUnit, merged_config)


@dataclass
class FlatMap(TransformStep[Any]):
    """
    A generic step for mapping and flattening (and therefore alerting the shape of) inputs to
    get outputs. Takes a single input to 0...N outputs.
    """

    step_type: StepType = StepType.FLAT_MAP


######################
# Complex Primitives #
######################


@dataclass
class Parser(ComplexStep, WithInput, Generic[TransformFuncReturnType]):
    """
    A step to decode bytes, deserialize the resulting message and validate it against the schema
    which corresponds to the message type provided. The message type should be one which
    is supported by sentry-kafka-schemas. See examples/ for usage, this step can be plugged in
    flexibly into a pipeline. Keep in mind, data up until this step will simply be bytes.

    Supports both JSON and protobuf.
    """

    msg_type: Type[TransformFuncReturnType]

    def convert(self) -> Step:
        return Map(
            name=self.name,
            function=msg_parser,
        )


@dataclass
class BatchParser(
    ComplexStep,
    WithInput,
    Generic[TransformFuncReturnType],
):
    msg_type: Type[TransformFuncReturnType]

    def convert(self) -> Step:
        return Map(
            name=self.name,
            function=batch_msg_parser,
        )


@dataclass
class Serializer(ComplexStep, WithInput):
    """
    A step to serialize and encode messages into bytes. These bytes can be written
    to sink data to a Kafka topic, for example. This step will need to precede a
    sink step which writes to Kafka.
    """

    dt_format: Optional[str] = None

    def convert(self) -> Step:
        return Map(
            name=self.name,
            function=msg_serializer,
        )


@dataclass
class Reducer(ComplexStep, WithInput, Generic[MeasurementUnit, InputType, OutputType]):
    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[Message[InputType], OutputType]]
    aggregate_backend: AggregationBackend[OutputType] | None = None
    group_by_key: GroupBy | None = None

    def convert(self) -> Step:
        return Aggregate(
            name=self.name,
            window=self.window,
            aggregate_func=self.aggregate_func,
            aggregate_backend=self.aggregate_backend,
            group_by_key=self.group_by_key,
        )
