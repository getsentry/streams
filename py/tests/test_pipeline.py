import os
from typing import Any

from pyflink.datastream import DataStream, DataStreamSink, StreamExecutionEnvironment
from sentry_streams.adapters.flink_adapter import FlinkAdapter
from sentry_streams.adapters.stream_adapter import StreamAdapter
from sentry_streams.pipeline import Pipeline, RuntimeTranslator
from sentry_streams.runner import iterate_edges


# Essentially identical to runner
# Checks basic input and output stream types
# of a simple Flink program
def test_pipeline() -> None:

    pipeline_globals: dict[str, Any] = {}

    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    config_file = os.path.join(
        "/".join(dir_path.split("/")[:-1]), "sentry_streams/example_config.py"
    )

    with open(config_file) as f:
        exec(f.read(), pipeline_globals)

    pipeline: Pipeline = pipeline_globals["pipeline"]
    p_graph = pipeline.graph

    libs_path = os.path.join("/".join(dir_path.split("/")[:-2]), "flink_libs")
    assert libs_path is not None, "FLINK_LIBS environment variable is not set"

    jar_file = os.path.join(os.path.abspath(libs_path), "flink-connector-kafka-3.4.0-1.20.jar")
    kafka_jar_file = os.path.join(os.path.abspath(libs_path), "kafka-clients-3.4.0.jar")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": "localhost:9092",
    }

    # This will not be harcdoded in the future
    runtime_config: StreamAdapter = FlinkAdapter(environment_config, env)
    translator = RuntimeTranslator(runtime_config)

    pipeline.set_translator(translator)

    step_streams = {}

    for source in p_graph.sources:
        print(f"Apply source: {source.name}")
        env_source = source.apply_source()
        assert type(env_source) is DataStream
        step_streams[source.name] = env_source
        output_stream = iterate_edges(step_streams, p_graph)

    assert type(output_stream) is DataStreamSink
