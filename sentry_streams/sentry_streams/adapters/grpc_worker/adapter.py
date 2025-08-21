"""
gRPC Worker Adapter for sentry_streams

This adapter provides integration with the flink_worker gRPC service.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, MutableMapping, Type, cast

from sentry_streams.pipeline.pipeline import Batch

from flink_worker.service import create_server

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.grpc_worker.chain import (
    BatchChainStep,
    FilterChainStep,
    FlatMapChainStep,
    MapChainStep,
    PickleStep,
)
from sentry_streams.adapters.grpc_worker.chain_builder import ChainBuilder
from sentry_streams.adapters.stream_adapter import (
    Broadcast,
    ComplexStep,
    Filter,
    FlatMap,
    Map,
    PipelineConfig,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Source,
    StreamAdapter,
)
from sentry_streams.config_types import StepConfig
from sentry_streams.pipeline.function_template import InputType, OutputType
from sentry_streams.pipeline.window import MeasurementUnit

logger = logging.getLogger(__name__)


class GRPCWorkerAdapter(StreamAdapter[Route, Route]):
    """
    A StreamAdapter implementation that uses the flink_worker gRPC service.

    This adapter is currently empty and serves as a placeholder for future implementation.
    """

    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        port: int,
    ) -> None:
        super().__init__()
        self.steps_config = steps_config
        self.__chains = ChainBuilder()
        self.__schemas: MutableMapping[str, str] = {}
        self.__port = port

    @classmethod
    def build(cls, config: PipelineConfig) -> GRPCWorkerAdapter[Route, Route]:
        steps_config = config["pipeline"]["segments"][0]["steps_config"]
        port = config["env"]["port"]

        return cls(steps_config, port)

    def complex_step_override(
        self,
    ) -> dict[Type[ComplexStep[Any, Any]], Callable[[ComplexStep[Any, Any]], Route]]:
        """
        Override for complex steps.

        Returns:
            Empty dictionary as no complex steps are currently handled
        """
        return {

        }



    def __init_chain(self, stream: Route, step_name: str, step_config: Mapping[str, Any], force: bool = False) -> None:
        if step_config.get("starts_segment") or not self.__chains.exists(stream) or force:
            if self.__chains.exists(stream):
                logger.info(f"Finalizing chain for {stream}")
                self.__chains.add_step(stream, PickleStep())
                self.__chains.finalize_chain(stream)

            logger.info(f"Initializing chain for {stream} with step {step_name}")
            self.__chains.init_chain(stream, step_name, self.__schemas[stream.source])

    def source(self, step: Source[Any]) -> Route:
        self.__schemas[step.name] = step.stream_name
        return Route(step.name, [])

    def sink(self, step: Sink[Any], stream: Route) -> Route:
        return stream

    def map(self, step: Map[Any, Any], stream: Route) -> Route:
        self.__init_chain(stream, step.name, self.steps_config.get(step.name, {}))
        self.__chains.add_step(stream, MapChainStep(step.resolved_function))
        return stream

    def flat_map(self, step: FlatMap[Any, Any], stream: Route) -> Route:
        self.__init_chain(stream, step.name, self.steps_config.get(step.name, {}))
        self.__chains.add_step(stream, FlatMapChainStep(step.resolved_function))
        return stream

    def filter(self, step: Filter[Any], stream: Route) -> Route:
        self.__init_chain(stream, step.name, self.steps_config.get(step.name, {}))
        self.__chains.add_step(stream, FilterChainStep(step.resolved_function))
        return stream

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: Route,
    ) -> Route:
        if isinstance(step, Batch):
            batch = cast(Batch, step)
            self.__init_chain(stream, step.name, self.steps_config.get(step.name, {}), force=True)
            self.__chains.add_step(stream, BatchChainStep(
                batch_size=batch.batch_size,
                batch_time_sec=batch.batch_timedelta.total_seconds()
            ))
            return stream
        else:
            raise NotImplementedError("Reduce not yet implemented in GRPCWorkerAdapter")

    def router(
        self,
        step: Router[RoutingFuncReturnType, Any],
        stream: Route,
    ) -> Mapping[str, Route]:
        raise NotImplementedError("Reduce not yet implemented in GRPCWorkerAdapter")

    def broadcast(
        self,
        step: Broadcast[Any],
        stream: Route,
    ) -> Mapping[str, Route]:
        raise NotImplementedError("Broadcast not yet implemented in GRPCWorkerAdapter")

    def run(self) -> None:
        """
        Starts the pipeline.

        Currently not implemented.
        """
        segments = self.__chains.finalize_all()
        for segment_id, segment in segments.items():
            logger.info(f"Running segment {segment_id} with {len(segment.steps)} steps")

        port = self.__port
        self.__server = create_server(segments, {}, port)
        self.__server.start()

        logger.info(f"Flink Worker gRPC server started on port {port}")
        logger.info("Press Ctrl+C to stop the server")

        # Keep the server running
        self.__server.wait_for_termination()

    def shutdown(self) -> None:
        """
        Cleanly shuts down the application.

        Currently not implemented.
        """
        logger.info("Shutting down server")
        self.__server.stop(0)
