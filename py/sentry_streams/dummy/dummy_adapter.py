from typing import Any, Self

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline import Map, Step


class DummyAdapter(StreamAdapter):
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

    def run(self) -> None:
        pass
