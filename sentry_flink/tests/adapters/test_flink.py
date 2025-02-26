import pytest
from sentry_streams.adapters.stream_adapter import PipelineConfig

from sentry_flink.flink.flink_adapter import FlinkAdapter


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    return {
        "broker": "localhost:9092",
        "topics": {"test_topic": "test_topic"},
        "flink": {"parallelism": 2, "kafka_connect_lib_path": "/path/to/libs"},
    }


def test_build(pipeline_config: PipelineConfig) -> None:
    adapter = FlinkAdapter.build(pipeline_config)
    assert adapter.env.get_parallelism() == 2
