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

from sentry_streams.pipeline.pipeline import (
    Filter,
    FlatMapStep,
    Map,
    Reduce,
    Sink,
    Source,
    Step,
    StepType,
)

PipelineConfig = Mapping[str, Any]


Stream = TypeVar("Stream")
StreamSink = TypeVar("StreamSink")


class StreamAdapter(ABC, Generic[Stream, StreamSink]):
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
    def source(self, step: Source) -> Stream:
        """
        Builds a stream source for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Sink, stream: Stream) -> StreamSink:
        """
        Builds a stream sink for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Map, stream: Stream) -> Stream:
        """
        Builds a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def flat_map(self, step: FlatMapStep, stream: Stream) -> Stream:
        """
        Builds a flat-map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, step: Filter, stream: Stream) -> Stream:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def reduce(
        self,
        step: Reduce,
        stream: Stream,
    ) -> Stream:
        """
        Build a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        """
        Starts the pipeline
        """
        raise NotImplementedError


class RuntimeTranslator(Generic[Stream, StreamSink]):
    """
    A runtime-agnostic translator
    which can apply the physical steps and transformations
    to a stream. Uses a StreamAdapter to determine
    which underlying runtime to translate to.
    """

    def __init__(self, runtime_adapter: StreamAdapter[Stream, StreamSink]):
        self.adapter = runtime_adapter

    def translate_step(
        self, step: Step, stream: Optional[Stream] = None
    ) -> Union[Stream, StreamSink]:
        assert hasattr(step, "step_type")
        step_type = step.step_type

        if step_type is StepType.SOURCE:
            assert isinstance(step, Source)
            return self.adapter.source(step)

        elif step_type is StepType.SINK:
            assert isinstance(step, Sink) and stream is not None
            return self.adapter.sink(step, stream)

        elif step_type is StepType.MAP:
            assert isinstance(step, Map) and stream is not None
            return self.adapter.map(step, stream)

        elif step_type is StepType.FLAT_MAP:
            assert isinstance(step, FlatMapStep) and stream is not None
            return self.adapter.flat_map(step, stream)

        elif step_type is StepType.REDUCE:
            assert isinstance(step, Reduce) and stream is not None
            return self.adapter.reduce(step, stream)

        elif step_type is StepType.FILTER:
            assert isinstance(step, Filter) and stream is not None
            return self.adapter.filter(step, stream)

        else:
            assert_never(step_type)
