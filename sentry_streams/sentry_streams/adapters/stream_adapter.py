from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional, Self, assert_never

from sentry_streams.pipeline import Filter, Map, Sink, Source, Step, StepType

PipelineConfig = Mapping[str, Any]


class StreamAdapter(ABC):
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
    def source(self, step: Source) -> Any:
        """
        Builds a stream source for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Sink, stream: Any) -> Any:
        """
        Builds a stream sink for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Map, stream: Any) -> Any:
        """
        Builds a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, step: Filter, stream: Any) -> Any:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        """
        Starts the pipeline
        """
        raise NotImplementedError


class RuntimeTranslator:
    """
    A runtime-agnostic translator
    which can apply the physical steps and transformations
    to a stream. Uses a StreamAdapter to determine
    which underlying runtime to translate to.
    """

    def __init__(self, runtime_adapter: StreamAdapter):
        self.adapter = runtime_adapter

    def translate_step(self, step: Step, stream: Optional[Any] = None) -> Any:
        assert hasattr(step, "step_type")
        step_type = step.step_type

        if step_type is StepType.SOURCE:
            assert isinstance(step, Source)
            return self.adapter.source(step)

        elif step_type is StepType.SINK:
            assert isinstance(step, Sink)
            return self.adapter.sink(step, stream)

        elif step_type is StepType.MAP:
            assert isinstance(step, Map)
            return self.adapter.map(step, stream)

        elif step_type is StepType.FILTER:
            assert isinstance(step, Filter)
            return self.adapter.filter(step, stream)

        else:
            assert_never(step_type)
