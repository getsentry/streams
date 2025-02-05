from abc import ABC, abstractmethod
from typing import Any, Optional

from sentry_streams.pipeline import Step


class StreamAdapter(ABC):
    "Class for mapping sentry_streams APIs and primitives to runtime-specific ones"

    @abstractmethod
    def source(self, step: Step) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Step, stream: Any) -> Any:
        raise NotImplementedError


class RuntimeTranslator:
    def __init__(self, runtime_adapter: StreamAdapter):
        self.adapter = runtime_adapter

    def translate_step(self, step: Step, stream: Optional[Any] = None) -> Any:
        assert hasattr(step, "step_type")
        translated_fn = getattr(self.adapter, step.step_type)

        if stream:
            return translated_fn(step, stream)
        else:
            return translated_fn(step)
