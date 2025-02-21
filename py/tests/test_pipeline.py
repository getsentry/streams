import json
import os
from typing import Any, Generator, MutableMapping

import pytest
from pyflink.datastream import StreamExecutionEnvironment

from sentry_streams.adapters.stream_adapter import RuntimeTranslator, StreamAdapter
from sentry_streams.flink.flink_adapter import FlinkAdapter
from sentry_streams.pipeline import KafkaSink, KafkaSource, Map, Pipeline
from sentry_streams.runner import iterate_edges


@pytest.fixture(autouse=True)
def setup_basic_flink_env() -> (
    Generator[tuple[StreamExecutionEnvironment, RuntimeTranslator], None, None]
):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    libs_path = os.path.join("/".join(dir_path.split("/")[:-2]), "flink_libs")
    assert libs_path is not None, "FLINK_LIBS environment variable is not set"

    jar_file = os.path.join(os.path.abspath(libs_path), "flink-sql-connector-kafka-3.4.0-1.20.jar")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{jar_file}")

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": "localhost:9092",
    }

    runtime_config: StreamAdapter = FlinkAdapter(environment_config, env)
    translator = RuntimeTranslator(runtime_config)

    setup_data = (env, translator)

    yield setup_data


def basic() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:
    pipeline = Pipeline()

    source = KafkaSource(
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[source],
        logical_topic="transformed-events",
    )

    expected = {
        "nodes": [
            {
                "id": 1,
                "type": "Source: Custom Source",
                "pact": "Data Source",
                "contents": "Source: Custom Source",
                "parallelism": 1,
            },
            {
                "id": 3,
                "type": "Sink: Writer",
                "pact": "Operator",
                "contents": "Sink: Writer",
                "parallelism": 1,
                "predecessors": [{"id": 1, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "id": 5,
                "type": "Sink: Committer",
                "pact": "Operator",
                "contents": "Sink: Committer",
                "parallelism": 1,
                "predecessors": [{"id": 3, "ship_strategy": "FORWARD", "side": "second"}],
            },
        ]
    }

    return (pipeline, expected)


def basic_map() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:
    pipeline = Pipeline()

    source = KafkaSource(
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function="sentry_streams.sample_function.EventsPipelineMapFunction.simple_map",
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        logical_topic="transformed-events",
    )

    expected = {
        "nodes": [
            {
                "id": 7,
                "type": "Source: Custom Source",
                "pact": "Data Source",
                "contents": "Source: Custom Source",
                "parallelism": 1,
            },
            {
                "id": 8,
                "type": "Map",
                "pact": "Operator",
                "contents": "Map",
                "parallelism": 1,
                "predecessors": [{"id": 7, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "id": 10,
                "type": "Sink: Writer",
                "pact": "Operator",
                "contents": "Sink: Writer",
                "parallelism": 1,
                "predecessors": [{"id": 8, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "id": 12,
                "type": "Sink: Committer",
                "pact": "Operator",
                "contents": "Sink: Committer",
                "parallelism": 1,
                "predecessors": [{"id": 10, "ship_strategy": "FORWARD", "side": "second"}],
            },
        ]
    }

    return (pipeline, expected)


@pytest.mark.parametrize("pipeline,expected_plan", [basic(), basic_map()])
def test_pipeline(
    setup_basic_flink_env: tuple[StreamExecutionEnvironment, RuntimeTranslator],
    pipeline: Pipeline,
    expected_plan: MutableMapping[str, list[dict[str, Any]]],
) -> None:

    env, translator = setup_basic_flink_env

    iterate_edges(pipeline, translator)

    assert json.loads(env.get_execution_plan()) == expected_plan


def bad_map() -> Pipeline:
    pipeline = Pipeline()

    source = KafkaSource(
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function="sentry_streams.unknown_module.EventsPipelineMapFunction.simple_map",
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        logical_topic="transformed-events",
    )

    return pipeline


@pytest.mark.parametrize("pipeline", [bad_map()])
def test_map_import(
    setup_basic_flink_env: tuple[StreamExecutionEnvironment, RuntimeTranslator],
    pipeline: Pipeline,
) -> None:
    _, translator = setup_basic_flink_env

    with pytest.raises(ImportError):
        iterate_edges(pipeline, translator)
