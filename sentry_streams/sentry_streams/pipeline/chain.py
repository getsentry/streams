from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Callable,
    Generic,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_parser import msg_parser, msg_serializer
from sentry_streams.pipeline.pipeline import (
    Aggregate,
)
from sentry_streams.pipeline.pipeline import Batch as BatchStep
from sentry_streams.pipeline.pipeline import (
    Branch,
    Broadcast,
)
from sentry_streams.pipeline.pipeline import Filter as FilterStep
from sentry_streams.pipeline.pipeline import FlatMap as FlatMapStep
from sentry_streams.pipeline.pipeline import GCSSink as GCSSinkStep
from sentry_streams.pipeline.pipeline import Map as MapStep
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    Router,
    Step,
)
from sentry_streams.pipeline.pipeline import StreamSink as StreamSinkStep
from sentry_streams.pipeline.pipeline import (
    StreamSource,
)
from sentry_streams.pipeline.window import MeasurementUnit, Window

TRoute = TypeVar("TRoute")
TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


@dataclass
class Applier(ABC, Generic[TIn, TOut]):
    """
    Defines a primitive that can be applied on a stream.
    Instances of these class represent a step in the pipeline and
    contains the metadata needed by the adapter to add the step
    to the pipeline itself.

    This class is primarily syntactic sugar to avoid having tons
    of methods in the `Chain` class and still allow some customization
    of the primitives.
    """

    @abstractmethod
    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        """
        Build a pipeline step and wires it to the Pipeline.

        This method will go away once the old syntax will be retired.
        """
        raise NotImplementedError


@dataclass
class Map(Applier[Message[TIn], Message[TOut]], Generic[TIn, TOut]):
    function: Union[Callable[[Message[TIn]], TOut], str]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return MapStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class Filter(Applier[Message[TIn], Message[TIn]], Generic[TIn]):
    function: Union[Callable[[Message[TIn]], bool], str]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return FilterStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class FlatMap(Applier[Message[MutableSequence[TIn]], Message[TOut]], Generic[TIn, TOut]):
    function: Union[
        Callable[[Message[MutableSequence[TIn]]], TOut], str
    ]  # TODO: Consider making this type an Iterable rather than MutableSequence

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return FlatMapStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class Reducer(
    Applier[Message[InputType], Message[OutputType]],
    Generic[MeasurementUnit, InputType, OutputType],
):
    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[Message[InputType], OutputType]]
    aggregate_backend: AggregationBackend[OutputType] | None = None
    group_by_key: GroupBy | None = None

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return Aggregate(
            name=name,
            ctx=ctx,
            inputs=[previous],
            window=self.window,
            aggregate_func=self.aggregate_func,
            aggregate_backend=self.aggregate_backend,
            group_by_key=self.group_by_key,
        )


@dataclass
class Parser(Applier[Message[bytes], Message[TOut]], Generic[TOut]):
    """
    A step to decode bytes, deserialize the resulting message and validate it against the schema
    which corresponds to the message type provided. The message type should be one which
    is supported by sentry-kafka-schemas. See examples/ for usage, this step can be plugged in
    flexibly into a pipeline. Keep in mind, data up until this step will simply be bytes.

    Supports both JSON and protobuf.
    """

    msg_type: Type[TOut]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return MapStep(
            name=name,
            ctx=ctx,
            inputs=[previous],
            function=msg_parser,
        )


@dataclass
class Serializer(Applier[Message[TIn], bytes], Generic[TIn]):
    """
    A step to serialize and encode messages into bytes. These bytes can be written
    to sink data to a Kafka topic, for example. This step will need to precede a
    sink step which writes to Kafka.
    """

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return MapStep(
            name=name,
            ctx=ctx,
            inputs=[previous],
            function=msg_serializer,
        )


@dataclass
class Batch(
    Applier[Message[InputType], Message[MutableSequence[InputType]]],
    Generic[MeasurementUnit, InputType],
):
    batch_size: MeasurementUnit

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return BatchStep(
            name=name,
            ctx=ctx,
            inputs=[previous],
            batch_size=self.batch_size,
        )


@dataclass
class Sink(ABC):
    """
    Defines a generic Sink, which can be extended by special
    types of Sinks. See examples/ to see how different kinds
    of Sinks are plugged into a pipeline.
    """

    @abstractmethod
    def build_sink(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        """
        Build a pipeline step and wires it to the Pipeline.

        This method will go away once the old syntax will be retired.
        """
        raise NotImplementedError


@dataclass
class StreamSink(Sink):
    stream_name: str

    def build_sink(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return StreamSinkStep(name=name, ctx=ctx, inputs=[previous], stream_name=self.stream_name)


@dataclass
class GCSSink(Sink):
    bucket: str
    object_file: str

    def build_sink(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return GCSSinkStep(
            name=name, ctx=ctx, inputs=[previous], bucket=self.bucket, object_file=self.object_file
        )


class Chain(Pipeline):
    """
    A pipeline that terminates with a branch or a sink. Which means a pipeline
    we cannot append further steps on.

    This type exists so the type checker can prevent us from reaching an
    invalid state.
    """

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name


class ExtensibleChain(Chain, Generic[TIn]):
    """
    Defines a streaming pipeline or a segment of a pipeline by chaining
    operators that define steps via function calls.

    A Chain is a pipeline that starts with a source, follows a number of
    steps. Some steps are operators that perform processing on a stream.
    Other steps manage the pipeline topology: sink, broadcast, route.

    Example:

    .. code-block:: python

        pipeline = streaming_source("myinput", "events") # Starts the pipeline
            .apply("transform1", Map(lambda msg: msg)) # Performs an operation
            .route( # Branches the pipeline
                "route_to_one",
                routing_function=routing_func,
                routes={
                    Routes.ROUTE1: segment(name="route1") # Creates a branch
                    .apply("transform2", Map(lambda msg: msg))
                    .sink("myoutput1", StreamSink("transformed-events-2")),
                    Routes.ROUTE2: segment(name="route2")
                    .apply("transform3", Map(lambda msg: msg))
                    .sink("myoutput2", StreamSink("transformed-events3")),
                }
            )

    """

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.__edge: Step | None = None

    def _add_start(self, start: Step) -> None:
        self.__edge = start

    def apply(self, name: str, applier: Applier[TIn, TOut]) -> ExtensibleChain[TOut]:
        """
        Apply a transformation to the stream. The transformation is
        defined via an `Applier`.

        Operations can change the cardinality of the messages in the stream.
        Examples:
        - map performs a 1:1 transformation
        - filter performs a 1:0..1 transformation
        - flatMap performs a 1:n transformation
        - reduce performs a n:1 transformation
        """
        assert self.__edge is not None
        self.__edge = applier.build_step(name, self, self.__edge)
        return cast(ExtensibleChain[TOut], self)

    def broadcast(self, name: str, routes: Sequence[Chain]) -> Chain:
        """
        Forks the pipeline sending all messages to all routes.
        """
        assert self.__edge is not None
        Broadcast(
            name,
            ctx=self,
            inputs=[self.__edge],
            routes=[Branch(name=chain.name, ctx=self) for chain in routes],
        )
        for chain in routes:
            self.merge(other=chain, merge_point=chain.name)
        return self

    def route(
        self,
        name: str,
        routing_function: Callable[..., TRoute],
        routes: Mapping[TRoute, Chain],
    ) -> Chain:
        """
        Forks the pipeline sending each message to one of the branches.
        The `routing_function` parameter specifies the function that
        takes the message in and returns the route to send it to.
        """
        assert self.__edge is not None
        table = {branch: Branch(name=chain.name, ctx=self) for branch, chain in routes.items()}
        Router(
            name,
            ctx=self,
            inputs=[self.__edge],
            routing_function=routing_function,
            routing_table=table,
        )
        for branch in table:
            chain = routes[branch]
            self.merge(other=chain, merge_point=chain.name)

        return self

    def sink(
        self,
        name: str,
        sink: Sink,
    ) -> Chain:
        """
        Terminates the pipeline.

        TODO: support anything other than StreamSink.
        """
        assert self.__edge is not None
        sink.build_sink(name=name, ctx=self, previous=self.__edge)
        return self


def segment(name: str, msg_type: Type[TIn]) -> ExtensibleChain[Message[TIn]]:
    """
    Creates a segment of a pipeline to be referenced in existing pipelines
    in route and broadcast steps.
    """
    pipeline: ExtensibleChain[Message[TIn]] = ExtensibleChain(name)
    pipeline._add_start(Branch(name=name, ctx=pipeline))
    return pipeline


def streaming_source(
    name: str, stream_name: str, header_filter: Optional[Tuple[str, bytes]] = None
) -> ExtensibleChain[Message[bytes]]:
    """
    Create a pipeline that starts with a StreamingSource.
    """
    pipeline: ExtensibleChain[Message[bytes]] = ExtensibleChain("root")
    source = StreamSource(
        name=name, ctx=pipeline, stream_name=stream_name, header_filter=header_filter
    )
    pipeline._add_start(source)
    return pipeline


def multi_chain(chains: Sequence[Chain]) -> Pipeline:
    """
    Creates a pipeline that contains multiple chains, where every
    chain is a portion of the pipeline that starts with a source
    and ends with multiple sinks.
    """
    pipeline = Pipeline()
    for chain in chains:
        pipeline.add(chain)
    return pipeline
