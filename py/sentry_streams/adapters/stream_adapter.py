from abc import ABC, abstractmethod
from typing import Any, Mapping


class StreamAdapter(ABC):
    "Class for mapping sentry_streams APIs to runtime-specific ones"

    @abstractmethod
    def source(self, step_config: Mapping[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step_config: Mapping[str, Any], stream: Any) -> Any:
        raise NotImplementedError
