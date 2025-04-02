import pytest
from sentry_streams.adapters.stream_adapter import PipelineConfig

from sentry_flink.flink.flink_adapter import FlinkAdapter


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    return {
        "env": {},
        "pipeline": {
            "segments": [
                {
                    "steps_config": {
                        "myinput": {"starts_segment": True, "bootstrap_servers": "localhost:9092"},
                        "kafkasink": {"bootstrap_servers": "localhost:9092"},
                        "kafkasink_1": {"bootstrap_servers": "localhost:9092"},
                        "kafkasink_2": {"bootstrap_servers": "localhost:9092"},
                        "kafkasink2": {"bootstrap_servers": "localhost:9092"},
                    }
                }
            ]
        },
    }


def test_build(pipeline_config: PipelineConfig) -> None:
    adapter = FlinkAdapter.build(pipeline_config)
    assert adapter.env.get_parallelism() == 2
