from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Callable,
    Generic,
    Mapping,
    MutableSequence,
    Sequence,
    TypeVar,
    Union,
)

from sentry_streams.pipeline.function_template import (
    Accumulator,
    AggregationBackend,
    GroupBy,
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
)
from sentry_streams.pipeline.pipeline import Batch as BatchStep
from sentry_streams.pipeline.pipeline import (
    Branch,
)
from sentry_streams.pipeline.pipeline import Filter as FitlerStep
from sentry_streams.pipeline.pipeline import FlatMap as FlatMapStep
from sentry_streams.pipeline.pipeline import Map as MapStep
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    Router,
    Step,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import MeasurementUnit, Window

TRoute = TypeVar("TRoute")

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


@dataclass
class Applier(ABC, Generic[TIn, TOut]):
    @abstractmethod
    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        raise NotImplementedError


@dataclass
class Map(Applier[TIn, TOut], Generic[TIn, TOut]):
    function: Union[Callable[[TIn], TOut], str]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return MapStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class Filter(Applier[TIn, TIn], Generic[TIn]):
    function: Union[Callable[[TIn], bool], str]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return FitlerStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class FlatMap(Applier[TIn, TOut], Generic[TIn, TOut]):
    function: Union[Callable[[TIn], TOut], str]

    def build_step(self, name: str, ctx: Pipeline, previous: Step) -> Step:
        return FlatMapStep(name=name, ctx=ctx, inputs=[previous], function=self.function)


@dataclass
class Reducer(Applier[InputType, OutputType], Generic[MeasurementUnit, InputType, OutputType]):
    window: Window[MeasurementUnit]
    aggregate_func: Callable[[], Accumulator[InputType, OutputType]]
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
class Batch(
    Applier[InputType, MutableSequence[InputType]],
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


class TerminatedChain(Pipeline):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name


class Chain(TerminatedChain):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.__edge: Step | None = None

    def _add_start(self, start: Step) -> None:
        self.__edge = start

    def apply(self, name: str, applier: Applier[TIn, TOut]) -> Chain:
        assert self.__edge is not None
        self.__edge = applier.build_step(name, self, self.__edge)
        return self

    def broadcast(self, name: str, routes: Sequence[TerminatedChain]) -> TerminatedChain:
        assert self.__edge is not None
        for chain in routes:
            self.merge(other=chain, merge_point=self.__edge.name)
        return self

    def route(
        self,
        name: str,
        routing_function: Callable[..., TRoute],
        routes: Mapping[TRoute, TerminatedChain],
    ) -> TerminatedChain:
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

    def sink(self, name: str, stream_name: str) -> TerminatedChain:
        assert self.__edge is not None
        StreamSink(name=name, ctx=self, inputs=[self.__edge], stream_name=stream_name)
        return self


def segment(name: str) -> Chain:
    pipeline: Chain = Chain(name)
    pipeline._add_start(Branch(name=name, ctx=pipeline))
    return pipeline


def streaming_source(name: str, stream_name: str) -> Chain:
    pipeline: Chain = Chain("root")
    source = StreamSource(
        name=name,
        ctx=pipeline,
        stream_name=stream_name,
    )
    pipeline._add_start(source)
    return pipeline
