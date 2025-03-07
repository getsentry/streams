from typing import Any, Self, TypeVar

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.pipeline import Filter, Map, Step

DummyInput = TypeVar("DummyInput")
DummyOutput = TypeVar("DummyOutput")


class DummyAdapter(StreamAdapter[DummyInput, DummyOutput]):
    """
    An infinitely scalable adapter that throws away all the data it gets.
    """

    def __init__(self, _: PipelineConfig) -> None:
        pass

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls(config)

    def source(self, step: Step) -> Any:
        return self

    def sink(self, step: Step, stream: Any) -> Any:
        return self

    def map(self, step: Map, stream: Any) -> Any:
        return self

    def filter(self, step: Filter, stream: Any) -> Any:
        return self

    def run(self) -> None:
        pass
