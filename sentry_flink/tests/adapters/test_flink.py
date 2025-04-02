from sentry_flink.flink.flink_adapter import FlinkAdapter

# @pytest.fixture
# def pipeline_config() -> PipelineConfig:
#     return {
#         "env": {},
#         "pipeline": {
#             "segments": [
#                 {
#                     "steps_config": {
#                         "myinput": {"starts_segment": True, "bootstrap_servers": "localhost:9092"},
#                         "kafkasink": {"bootstrap_servers": "localhost:9092"},
#                     }
#                 }
#             ]
#         },
#     }


def test_build() -> None:

    pipeline_config = {
        "env": {},
        "pipeline": {
            "segments": [
                {
                    "steps_config": {
                        "myinput": {"starts_segment": True, "bootstrap_servers": "localhost:9092"},
                        "kafkasink": {"bootstrap_servers": "localhost:9092"},
                    }
                }
            ]
        },
    }

    adapter = FlinkAdapter.build(pipeline_config)
    assert adapter.env.get_parallelism() == 1
