from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar, Union, assert_never

from sentry_streams.pipeline import Map, Reduce, Sink, Source, Step, StepType

# Generic types to define:
# A generic Stream type (can be a DataStream, Dataflow)
# A generic Message type


Stream = TypeVar("Stream")
StreamSink = TypeVar("StreamSink")


class StreamAdapter(ABC, Generic[Stream, StreamSink]):
    """
    A generic adapter for mapping sentry_streams APIs
    and primitives to runtime-specific ones. This can
    be extended to specific runtimes.
    """

    @abstractmethod
    def source(self, step: Source) -> Stream:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Sink, stream: Stream) -> StreamSink:
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Map, stream: Stream) -> Stream:
        raise NotImplementedError

    @abstractmethod
    def reduce(self, step: Reduce, stream: Stream) -> Stream:
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

        elif step_type is StepType.REDUCE:
            assert isinstance(step, Reduce) and stream is not None
            return self.adapter.reduce(step, stream)

        else:
            assert_never(step_type)
