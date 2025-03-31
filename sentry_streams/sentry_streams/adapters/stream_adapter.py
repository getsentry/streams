from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Generic,
    Mapping,
    Optional,
    Self,
    TypeVar,
    Union,
    assert_never,
)

from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    FlatMap,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Source,
    Step,
    StepType,
)
from sentry_streams.pipeline.window import MeasurementUnit

PipelineConfig = Mapping[str, Any]


StreamT = TypeVar("StreamT")
StreamSinkT = TypeVar("StreamSinkT")


class StreamAdapter(ABC, Generic[StreamT, StreamSinkT]):
    """
    A generic adapter for mapping sentry_streams APIs
    and primitives to runtime-specific ones. This can
    be extended to specific runtimes.
    """

    @classmethod
    @abstractmethod
    def build(cls, config: PipelineConfig) -> Self:
        """
        Create an adapter and instantiate the runtime specific context.

        This method exists so that we can define the type of the
        Pipeline config.

        Pipeline config contains the fields needed to instantiate the
        pipeline.
        #TODO: Provide a more structured way to represent config.
        # currently we rely on the adapter to validate the content while
        # there are a lot of configuration elements that can be adapter
        # agnostic.
        """
        raise NotImplementedError

    @abstractmethod
    def source(self, step: Source) -> StreamT:
        """
        Builds a stream source for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Sink, stream: StreamT) -> StreamSinkT:
        """
        Builds a stream sink for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Map, stream: StreamT) -> StreamT:
        """
        Builds a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def flat_map(self, step: FlatMap, stream: StreamT) -> StreamT:
        """
        Builds a flat-map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, step: Filter, stream: StreamT) -> StreamT:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: StreamT,
    ) -> StreamT:
        """
        Build a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def router(
        self,
        step: Router[RoutingFuncReturnType],
        stream: StreamT,
    ) -> Mapping[str, StreamT]:
        """
        Build a router operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def broadcast(
        self,
        step: Broadcast,
        stream: StreamT,
    ) -> Mapping[str, StreamT]:
        """
        Build a broadcast operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        """
        Starts the pipeline
        """
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        """
        Cleanly shutdown the application.
        """
        raise NotImplementedError


class RuntimeTranslator(Generic[StreamT, StreamSinkT]):
    """
    A runtime-agnostic translator
    which can apply the physical steps and transformations
    to a stream. Uses a StreamAdapter to determine
    which underlying runtime to translate to.
    """

    def __init__(self, runtime_adapter: StreamAdapter[StreamT, StreamSinkT]):
        self.adapter = runtime_adapter

    def translate_step(
        self, step: Step, stream: Optional[StreamT] = None
    ) -> Mapping[str, Union[StreamT, StreamSinkT]]:
        assert hasattr(step, "step_type")
        step_type = step.step_type
        step_name = step.name

        if step_type is StepType.SOURCE:
            assert isinstance(step, Source)
            return {step_name: self.adapter.source(step)}

        elif step_type is StepType.SINK:
            assert isinstance(step, Sink) and stream is not None
            return {step_name: self.adapter.sink(step, stream)}

        elif step_type is StepType.MAP:
            assert isinstance(step, Map) and stream is not None
            return {step_name: self.adapter.map(step, stream)}

        elif step_type is StepType.FLAT_MAP:
            assert isinstance(step, FlatMap) and stream is not None
            return {step_name: self.adapter.flat_map(step, stream)}

        elif step_type is StepType.REDUCE:
            assert isinstance(step, Reduce) and stream is not None
            return {step_name: self.adapter.reduce(step, stream)}

        elif step_type is StepType.FILTER:
            assert isinstance(step, Filter) and stream is not None
            return {step_name: self.adapter.filter(step, stream)}

        elif step_type is StepType.ROUTER:
            assert isinstance(step, Router) and stream is not None
            return self.adapter.router(step, stream)

        elif step_type is StepType.BROADCAST:
            assert isinstance(step, Broadcast) and stream is not None
            return self.adapter.broadcast(step, stream)

        else:
            assert_never(step_type)
