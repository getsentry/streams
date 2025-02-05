from abc import ABC, abstractmethod
from typing import Any

from sentry_streams.pipeline import Step


class StreamAdapter(ABC):
    "Class for mapping sentry_streams APIs and primitives to runtime-specific ones"

    @abstractmethod
    def source(self, step: Step) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Step, stream: Any) -> Any:
        raise NotImplementedError
