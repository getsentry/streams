"""
Flink Pipeline Adapter for sentry_streams

This adapter prints the YAML description of the pipeline without running anything.
"""

from __future__ import annotations

import logging
from typing import Mapping

import yaml

from sentry_streams.adapters.grpc_worker.adapter import GRPCWorkerAdapter
from sentry_streams.config_types import StepConfig

logger = logging.getLogger(__name__)


class FlinkPipelineAdapter(GRPCWorkerAdapter):
    """
    A StreamAdapter implementation that prints the YAML description of the pipeline without running anything.

    This adapter is useful for debugging and understanding pipeline configurations.
    """

    def __init__(
        self,
        steps_config: Mapping[str, StepConfig],
        pipeline_config: dict,
    ) -> None:
        # GRPCWorkerAdapter expects (steps_config, port), so we'll use a default port
        super().__init__(steps_config, 8080)
        self.steps_config = steps_config
        self.pipeline_config = pipeline_config
        self.segments_descriptor: list = []

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
            yaml_output = yaml.dump(self.chains.get_descriptors())
            print(yaml_output)
        except Exception as e:
            logger.error(f"Failed to format YAML: {e}")
            logger.info(f"Raw config: {self.pipeline_config}")

        logger.info("=== END PIPELINE DESCRIPTION ===")
        logger.info("Pipeline description printed. No execution performed.")
