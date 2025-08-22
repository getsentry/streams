"""
Flink Pipeline Adapter for sentry_streams

This adapter prints the YAML description of the pipeline without running anything.
"""

from .adapter import FlinkPipelineAdapter

__all__ = ["FlinkPipelineAdapter"]
