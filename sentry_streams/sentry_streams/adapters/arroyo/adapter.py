from __future__ import annotations

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import Filter, Map, Reduce, Sink, Source
from sentry_streams.pipeline.window import MeasurementUnit


class ArroyoAdapter(StreamAdapter[Route, Route]):

    @classmethod
    def build(cls, config: PipelineConfig) -> ArroyoAdapter:
        raise NotImplementedError

    def source(self, step: Source) -> Route:
        """
        Builds a stream source for the platform the adapter supports.
        """
        raise NotImplementedError

    def sink(self, step: Sink, stream: Route) -> Route:
        """
        Builds a stream sink for the platform the adapter supports.
        """
        raise NotImplementedError

    def map(self, step: Map, stream: Route) -> Route:
        """
        Builds a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def filter(self, step: Filter, stream: Route) -> Route:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: Route,
    ) -> Route:
        """
        Build a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    def run(self) -> None:
        """
        Starts the pipeline
        """
        raise NotImplementedError
