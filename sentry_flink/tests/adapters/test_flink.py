import importlib

import pytest
import yaml
from sentry_streams.adapters.stream_adapter import PipelineConfig

from sentry_flink.flink.flink_adapter import FlinkAdapter


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    config_file = (
        importlib.resources.files("sentry_streams") / "deployment_config" / "test_flink_config.yaml"
    )
    with config_file.open("r") as file:
        environment_config = yaml.safe_load(file)

    return environment_config


def test_build(pipeline_config: PipelineConfig) -> None:
    adapter = FlinkAdapter.build(pipeline_config)
    assert adapter.env.get_parallelism() == 4
