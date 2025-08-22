"""
Flink Pipeline Adapter for sentry_streams

This adapter prints the YAML description of the pipeline without running anything.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping, MutableMapping

import yaml

from sentry_streams.adapters.grpc_worker.adapter import GRPCWorkerAdapter
from sentry_streams.config_types import StepConfig
from sentry_streams.pipeline.pipeline import Sink, Source, StreamSink, StreamSource
from sentry_streams.rust_streams import Route

logger = logging.getLogger(__name__)


class FlinkPipelineAdapter(GRPCWorkerAdapter):
    """
    A StreamAdapter implementation that prints the YAML description of the pipeline without running anything.

    This adapter is useful for debugging and understanding pipeline configurations.
    """

    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        port: int,
    ) -> None:
        # GRPCWorkerAdapter expects (steps_config, port), so we'll use a default port
        super().__init__(steps_config, port)
        self.steps_config = steps_config
        self.segments_descriptor: list = []
        self.sources: MutableMapping[str, MutableMapping[str, Any]] = {}
        self.sinks: MutableMapping[str, MutableMapping[str, Any]] = {}

    def source(self, step: Source[Any]) -> Route:
        # Handle different types of Source classes

        assert isinstance(step, StreamSource), "Only streaming sources are supported"
        self.schemas[step.name] = step.stream_name
        self.sources[step.name] = dict(self.steps_config.get(step.name, {}))
        self.sources[step.name]["stream_name"] = step.stream_name

        return Route(step.name, [])

    def sink(self, step: Sink[Any], stream: Route) -> Route:
        assert isinstance(step, StreamSink), "Only streaming sinks are supported"
        self.sinks[step.name] = dict(self.steps_config.get(step.name, {}))
        self.sinks[step.name]["stream_name"] = step.stream_name
        return stream

    def run(self) -> None:
        """
        Prints the YAML description of the pipeline without running anything.
        """

        segments = self.chains.finalize_all()
        for segment_id, segment in segments.items():
            logger.info(f"Running segment {segment_id} with {len(segment.steps)} steps")

        logger.info("=== FLINK PIPELINE YAML DESCRIPTION ===")
        logger.info("Pipeline configuration:")

        # Print the pipeline config in a readable format
        try:
            yaml_output = yaml.dump(
                {
                    "sources": self.sources,
                    "sinks": self.sinks,
                    "steps": self.chains.get_descriptors(),
                }
            )
            print(yaml_output)
        except Exception as e:
            logger.error(f"Failed to format YAML: {e}")

        logger.info("=== END PIPELINE DESCRIPTION ===")
        logger.info("Pipeline description printed. No execution performed.")
