from abc import ABC, abstractmethod
from typing import Any, Optional, assert_never

from sentry_streams.pipeline import Step, StepType


class StreamAdapter(ABC):
    """
    A generic adapter for mapping sentry_streams APIs
    and primitives to runtime-specific ones. This can
    be extended to specific runtimes.
    """

    @abstractmethod
    def source(self, step: Step) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Step, stream: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Step, stream: Any) -> Any:
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
            return self.adapter.source(step)

        elif step_type is StepType.SINK:
            return self.adapter.sink(step, stream)

        elif step_type is StepType.MAP:
            return self.adapter.map(step, stream)

        else:
            assert_never(step_type)
