from __future__ import annotations

from sentry_streams.pipeline.pipeline import Pipeline, KafkaSource, Step, Map
from typing import Union, Callable, TypeVar, Any, Sequence, Mapping, Iterable

T = TypeVar("T")
function_ref = Union[Callable[..., T], str]


class Reducer:
    pass


class Chain(Pipeline):
    def __init__(self) -> None:
        self.__edge: Step | None = None

    def _add_start(self, start: Step) -> None:
        self.__edge = start

    def map(self, name: str, function: Union[Callable[..., Any], str]) -> Chain:
        assert self.__edge is not None
        self.__edge = Map(name=name, ctx=self, inputs=[self.__edge], function=function)
        return self

    def apply(self, name: str, function: Union[Callable[..., Iterable[Any]], str]) -> Chain:
        return self

    def reduce(self, name: str, reducer: Reducer) -> Chain:
        return self

    def broadcast(self, name: str, routes: Sequence[Pipeline]) -> Pipeline:
        return self

    def branch(
        self,
        name: str,
        routing_function: Union[Callable[..., T], str],
        routes: Mapping[T, Pipeline],
    ) -> Pipeline:
        return self

    def sink(self) -> Pipeline:
        return self


def branch() -> Chain:
    pass


def streaming_source(name: str, logical_topic: str) -> Chain:
    pipeline = Chain()
    KafkaSource(
        name=name,
        ctx=pipeline,
        logical_topic=logical_topic,
    )
    return pipeline
