from abc import ABC, abstractmethod
from typing import Any, Mapping


class StreamAdapter(ABC):

    @abstractmethod
    def source(self, step_config: Mapping[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def sink(self, step_config: Mapping[str, Any], stream: Any) -> Any:
        raise NotImplementedError
