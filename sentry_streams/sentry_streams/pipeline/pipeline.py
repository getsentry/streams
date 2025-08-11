from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from functools import partial
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from sentry_streams.modules import get_module
from sentry_streams.pipeline.batch import BatchBuilder
from sentry_streams.pipeline.datatypes import DataType
from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_codecs import (
    ParquetCompression,
    batch_msg_parser,
    msg_parser,
    msg_serializer,
    resolve_polars_schema,
    serialize_to_parquet,
)
from sentry_streams.pipeline.rust_function_protocol import InternalRustFunction
from sentry_streams.pipeline.window import MeasurementUnit, TumblingWindow, Window

RoutingFuncReturnType = TypeVar("RoutingFuncReturnType")
TransformFuncReturnType = TypeVar("TransformFuncReturnType")
TIn = TypeVar("TIn")
TOut = TypeVar("TOut")
TNewOut = TypeVar("TNewOut")
TBranch = TypeVar("TBranch")

RUST_FUNCTION_VERSION = 1


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


class Pipeline(Generic[TOut]):
    """
    A graph representing the connections between
    logical Steps.
    """

    def __init__(self, source: Source[TOut] | Branch[TOut]) -> None:
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

    def _merge(self, other: Pipeline[TOut], merge_point: str) -> None:
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

    def apply(
        self, step: Union[Transform[TOut, TNewOut], ComplexStep[TOut, TNewOut]]
    ) -> Pipeline[TNewOut]:
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step.register(self, self.__last_added_step)
        self.__last_added_step = step
        return cast(Pipeline[TNewOut], self)

    def sink(self, step: Sink[TOut]) -> Pipeline[TOut]:
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step.register(self, self.__last_added_step)
        self._closed = True
        return self

    def broadcast(self, name: str, routes: Sequence[Pipeline[TOut]]) -> Pipeline[TOut]:
        """
        Broadcast a message to multiple branches. Adding a broadcast step will close the pipeline, since
        more steps can't be added after it. Thus it expects that all the branches are fully defined.
        """
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step = Broadcast[TOut](name=name, routes=routes)
        step.register(self, self.__last_added_step)
        self.__last_added_step = step
        self._closed = True
        return self

    def route(
        self,
        name: str,
        routing_function: Callable[[Message[TOut]], RoutingFuncReturnType],
        routing_table: Mapping[RoutingFuncReturnType, Pipeline[TOut]],
    ) -> Pipeline[TOut]:
        """
        Route a message to a specific branch based on a routing function. Adding a router step will close the pipeline, since
        more steps can't be added after it. Thus it expects that all the branches are fully defined.
        """
        assert not self._closed, "Cannot add to a pipeline after it has been closed"
        step = Router[RoutingFuncReturnType, TOut](
            name=name, routing_function=routing_function, routing_table=routing_table
        )
        step.register(self, self.__last_added_step)
        self.__last_added_step = step
        self._closed = True
        return self


def streaming_source(name: str, stream_name: str) -> Pipeline[bytes]:
    """
    Used to create a new pipeline with a streaming source, where the stream_name is the
    name of the Kafka topic to read from.
    """
    return Pipeline(StreamSource(name=name, stream_name=stream_name))


def branch(name: str) -> Pipeline[Any]:
    """
    Used to create a new pipeline with a branch as the root step. This pipeline can then be added as part of
    a router or broadcast step.
    """
    return Pipeline(Branch[Any](name=name))


@dataclass
class Step:
    """
    A generic Step, whose incoming
    and outgoing edges are registered
    against a Pipeline.
    """

    name: str

    def register(self, ctx: Pipeline[Any], previous: Step) -> None:
        ctx.register(self)

    def override_config(self, loaded_config: Mapping[str, Any]) -> None:
        """
        Steps can implement custom overriding logic
        """
        pass


@dataclass
class Source(Step, Generic[TOut]):
    """
    A generic Source that produces output of type TOut.
    """


@dataclass
class StreamSource(Source[bytes]):
    """
    A Source which reads from Kafka.
    """

    stream_name: str
    header_filter: Optional[Tuple[str, bytes]] = None
    step_type: StepType = StepType.SOURCE

    def register(self, ctx: Pipeline[bytes], previous: Step) -> None:
        super().register(ctx, previous)


@dataclass
class WithInput(Step, Generic[TIn]):
    """
    A generic Step representing a logical
    step which has inputs of type TIn.
    """

    def register(self, ctx: Pipeline[Any], previous: Step) -> None:
        super().register(ctx, previous)
        ctx.register_edge(previous, self)


@dataclass
class ComplexStep(WithInput[TIn], Generic[TIn, TOut]):
    """
    A wrapper around a simple step that allows for syntactic sugar/more complex steps.
    The convert() function must return a simple step.
    ComplexStep[TIn, TOut] represents a step that transforms TIn to TOut.
    """

    @abstractmethod
    def convert(self) -> Transform[TIn, TOut]:
        raise NotImplementedError()


@dataclass
class Transform(WithInput[TIn], Generic[TIn, TOut]):
    """
    A step that transforms input of type TIn to output of type TOut.
    """

    pass


@dataclass
class Sink(WithInput[TIn]):
    """
    A generic Sink that consumes input of type TIn.
    """


@dataclass
class GCSSink(Sink[TIn]):
    """
    A Sink which writes to GCS
    """

    bucket: str
    object_generator: Callable[[], str]
    step_type: StepType = StepType.SINK


@dataclass
class StreamSink(Sink[TIn]):
    """
    A Sink which specifically writes to Kafka.
    """

    stream_name: str
    step_type: StepType = StepType.SINK


@dataclass
class FunctionTransform(Transform[TIn, TOut], Generic[TIn, TOut]):
    """
    A transform step that applies a function to transform TIn to TOut.
    function: supports reference to a function using dot notation, or a Callable
    """

    function: Union[Callable[[Message[TIn]], TOut], str]
    step_type: StepType

    @property
    def resolved_function(self) -> Callable[[Message[TIn]], TOut]:
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
        imported_func = cast(Callable[[Message[TIn]], TOut], getattr(imported_cls, fn))
        function_callable = imported_func
        return function_callable

    def _validate_rust_function(self) -> Callable[[Message[TIn]], TOut] | None:
        func = self.resolved_function
        if not hasattr(func, "rust_function_version"):
            # not a rust function
            return None

        func = cast(InternalRustFunction[TIn, TOut], func)

        rust_function_version = func.rust_function_version()
        if rust_function_version != 1:
            raise TypeError(
                r"Invalid rust function version: {rust_function_version} -- if you are defining your own rust functions, maybe the version is out of date?"
            )

        return func

    def post_rust_function_validation(self, func: InternalRustFunction[TIn, TOut]) -> None:
        # Overridden in Filter step
        pass

    def __post_init__(self) -> None:
        self._validate_rust_function()


# Backward compatibility alias
TransformStep = FunctionTransform


@dataclass
class Map(FunctionTransform[TIn, TOut], Generic[TIn, TOut]):
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
class Filter(Transform[TIn, TIn], Generic[TIn]):
    """
    A simple Filter, taking a single input and either returning it or None as output.
    Note: Filter preserves the input type as output type.
    """

    function: Union[Callable[[Message[TIn]], bool], str]
    step_type: StepType = StepType.FILTER

    def post_rust_function_validation(self, func: InternalRustFunction[TIn, TOut]) -> None:
        output_type = func.output_type()
        if output_type != "bool":
            raise TypeError(
                f"Filter function {func} should return bool, " f"but returns {output_type}"
            )

    @property
    def resolved_function(self) -> Callable[[Message[TIn]], bool]:
        """
        Returns a callable of the filter function defined, or referenced in the
        this class
        """
        if callable(self.function):
            return self.function

        fn_path = self.function
        mod, cls, fn = fn_path.rsplit(".", 2)

        module = get_module(mod)

        imported_cls = getattr(module, cls)
        imported_func = cast(Callable[[Message[TIn]], bool], getattr(imported_cls, fn))
        function_callable = imported_func
        return function_callable


@dataclass
class Branch(WithInput[TIn], Generic[TIn]):
    """
    A Branch represents one branch in a pipeline, which is routed to
    by a Router. Note: Branch preserves the input type as output type.
    """

    step_type: StepType = StepType.BRANCH


@dataclass
class Router(WithInput[TIn], Generic[RoutingFuncReturnType, TIn]):
    """
    A step which takes a routing table of Branches and sends messages
    to those branches based on a routing function.
    Routing functions must only return a single output branch, routing
    to multiple branches simultaneously is not currently supported.
    """

    routing_function: Callable[[Message[TIn]], RoutingFuncReturnType]
    routing_table: Mapping[RoutingFuncReturnType, Pipeline[TIn]]
    step_type: StepType = StepType.ROUTER

    def register(self, ctx: Pipeline[TIn], previous: Step) -> None:
        super().register(ctx, previous)
        for pipeline in self.routing_table.values():
            ctx._merge(other=pipeline, merge_point=self.name)


@dataclass
class Broadcast(WithInput[TIn], Generic[TIn]):
    """
    A Broadcast step will forward messages to all downstream branches in a pipeline.
    """

    routes: Sequence[Pipeline[TIn]]
    step_type: StepType = StepType.BROADCAST

    def register(self, ctx: Pipeline[TIn], previous: Step) -> None:
        super().register(ctx, previous)
        for chain in self.routes:
            ctx._merge(other=chain, merge_point=self.name)


@dataclass
class Reduce(
    Transform[InputType, OutputType], ABC, Generic[MeasurementUnit, InputType, OutputType]
):
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
    def aggregate_fn(self) -> Callable[[], Accumulator[Message[InputType], OutputType]]:
        raise NotImplementedError()


@dataclass
class Aggregate(Reduce[MeasurementUnit, InputType, OutputType]):
    """
    A Reduce step which performs windowed aggregations. Can be keyed or non-keyed on the
    input stream. Supports an Accumulator-style aggregation which can have a configurable
    storage backend, for flushing intermediate aggregates.
    """

    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[Message[InputType], OutputType]]
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
    def aggregate_fn(self) -> Callable[[], Accumulator[Message[InputType], OutputType]]:
        return self.aggregate_func


BatchInput = TypeVar("BatchInput")


@dataclass
class Batch(
    Reduce[MeasurementUnit, InputType, MutableSequence[Tuple[InputType, Optional[str]]]],
    Generic[MeasurementUnit, InputType],
):
    """
    A step to Batch up the results of the prior step.

    Batch can be configured via batch size, which can be
    an event time duration or a count of events.
    """

    # TODO: Use concept of custom triggers to close window
    # by either size or time

    batch_size: int | None = None
    batch_timedelta: timedelta | None = None
    step_type: StepType = StepType.REDUCE

    def __post_init__(self) -> None:
        if self.batch_size is None and self.batch_timedelta is None:
            raise ValueError("At least one of batch_size or batch_timedelta must be set.")

    @property
    def group_by(self) -> Optional[GroupBy]:
        return None

    @property
    def windowing(self) -> Window[MeasurementUnit]:
        return TumblingWindow(self.batch_size, self.batch_timedelta)

    @property
    def aggregate_fn(
        self,
    ) -> Callable[
        [], Accumulator[Message[InputType], MutableSequence[Tuple[InputType, Optional[str]]]]
    ]:
        return cast(
            Callable[
                [],
                Accumulator[Message[InputType], MutableSequence[Tuple[InputType, Optional[str]]]],
            ],
            BatchBuilder,
        )

    def override_config(self, loaded_config: Mapping[str, Any]) -> None:
        if loaded_config.get("batch_size") is not None:
            self.batch_size = loaded_config.get("batch_size")

        if loaded_config.get("batch_timedelta") is not None:
            loaded_kwargs = loaded_config.get("batch_timedelta")
            assert isinstance(loaded_kwargs, Mapping)
            self.batch_timedelta = timedelta(**loaded_kwargs)


@dataclass
class FlatMap(Transform[TIn, TOut], Generic[TIn, TOut]):
    """
    A generic step for mapping and flattening (and therefore alerting the shape of) inputs to
    get outputs. Takes a single input to 0...N outputs.
    The function should return an Iterable[TOut], but the step itself outputs TOut.
    """

    function: Union[Callable[[Message[TIn]], Iterable[TOut]], str]
    step_type: StepType = StepType.FLAT_MAP

    @property
    def resolved_function(self) -> Callable[[Message[TIn]], Iterable[TOut]]:
        """
        Returns a callable of the flatmap function defined, or referenced in this class
        """
        if callable(self.function):
            return self.function

        fn_path = self.function
        mod, cls, fn = fn_path.rsplit(".", 2)

        module = get_module(mod)

        imported_cls = getattr(module, cls)
        imported_func = cast(Callable[[Message[TIn]], Iterable[TOut]], getattr(imported_cls, fn))
        function_callable = imported_func
        return function_callable


######################
# Complex Primitives #
######################


@dataclass
class Parser(ComplexStep[bytes, TransformFuncReturnType], Generic[TransformFuncReturnType]):
    """
    A step to decode bytes, deserialize the resulting message and validate it against the schema
    which corresponds to the message type provided. The message type should be one which
    is supported by sentry-kafka-schemas. See examples/ for usage, this step can be plugged in
    flexibly into a pipeline. Keep in mind, data up until this step will simply be bytes.

    Supports both JSON and protobuf.
    """

    def convert(self) -> Transform[bytes, TransformFuncReturnType]:
        return Map[bytes, TransformFuncReturnType](
            name=self.name,
            function=msg_parser,
        )


@dataclass
class BatchParser(
    ComplexStep[Sequence[bytes], Sequence[TransformFuncReturnType]],
    Generic[TransformFuncReturnType],
):

    def convert(self) -> Transform[Sequence[bytes], Sequence[TransformFuncReturnType]]:
        return Map[Sequence[bytes], Sequence[TransformFuncReturnType]](
            name=self.name,
            function=batch_msg_parser,
        )


@dataclass
class Serializer(ComplexStep[TIn, bytes], Generic[TIn]):
    """
    A step to serialize and encode messages into bytes. These bytes can be written
    to sink data to a Kafka topic, for example. This step will need to precede a
    sink step which writes to Kafka.
    """

    dt_format: Optional[str] = None

    def convert(self) -> Transform[TIn, bytes]:
        return Map[TIn, bytes](
            name=self.name,
            function=msg_serializer,
        )


@dataclass
class Reducer(ComplexStep[InputType, OutputType], Generic[MeasurementUnit, InputType, OutputType]):
    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[Message[InputType], OutputType]]
    aggregate_backend: AggregationBackend[OutputType] | None = None
    group_by_key: GroupBy | None = None

    def convert(self) -> Transform[InputType, OutputType]:
        return Aggregate[MeasurementUnit, InputType, OutputType](
            name=self.name,
            window=self.window,
            aggregate_func=self.aggregate_func,
            aggregate_backend=self.aggregate_backend,
            group_by_key=self.group_by_key,
        )


@dataclass
class ParquetSerializer(ComplexStep[MutableSequence[TIn], bytes], Generic[TIn]):
    # TODO: because BatchParser outputs a MutableSequence, to satisfy type checking this must also be a MutableSequence
    schema_fields: Mapping[str, DataType]
    compression: Optional[ParquetCompression] = "snappy"

    def convert(self) -> Transform[MutableSequence[TIn], bytes]:
        assert self.compression is not None
        polars_schema = resolve_polars_schema(self.schema_fields)

        serializer_fn = partial(
            serialize_to_parquet, polars_schema=polars_schema, compression=self.compression
        )
        return Map[MutableSequence[TIn], bytes](
            name=self.name,
            function=serializer_fn,
        )
