"""
Tests for the GRPCWorkerAdapter
"""

from sentry_streams.adapters.grpc_worker import GRPCWorkerAdapter
from sentry_streams.adapters.stream_adapter import PipelineConfig


def test_grpc_worker_adapter_build() -> None:
    """Test that GRPCWorkerAdapter can be built with a config."""
    config: PipelineConfig = {"env": {"port": 0}, "pipeline": {"segments": [{"steps_config": {}}]}}
    adapter = GRPCWorkerAdapter.build(config)

    assert isinstance(adapter, GRPCWorkerAdapter)
    assert adapter.complex_step_override() == {}
