import json
import os
import unittest

from pyflink.datastream import StreamExecutionEnvironment
from sentry_streams.adapters.stream_adapter import RuntimeTranslator, StreamAdapter
from sentry_streams.flink.flink_adapter import FlinkAdapter
from sentry_streams.pipeline import KafkaSink, KafkaSource, Map, Pipeline
from sentry_streams.runner import iterate_edges


class PipelineTestCase(unittest.TestCase):

    def setUp(self) -> None:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        libs_path = os.path.join("/".join(dir_path.split("/")[:-2]), "flink_libs")
        assert libs_path is not None, "FLINK_LIBS environment variable is not set"

        jar_file = os.path.join(os.path.abspath(libs_path), "flink-connector-kafka-3.4.0-1.20.jar")
        kafka_jar_file = os.path.join(os.path.abspath(libs_path), "kafka-clients-3.4.0.jar")

        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        self.env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

        # TODO: read from yaml file
        environment_config = {
            "topics": {
                "logical-events": "events",
                "transformed-events": "transformed-events",
            },
            "broker": "localhost:9092",
        }

        runtime_config: StreamAdapter = FlinkAdapter(environment_config, self.env)
        self.translator = RuntimeTranslator(runtime_config)

    def test_pipeline(self) -> None:

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

        iterate_edges(pipeline, self.translator)

        expected_plan = {
            "nodes": [
                {
                    "id": 1,
                    "type": "Source: Custom Source",
                    "pact": "Data Source",
                    "contents": "Source: Custom Source",
                    "parallelism": 1,
                },
                {
                    "id": 2,
                    "type": "Map",
                    "pact": "Operator",
                    "contents": "Map",
                    "parallelism": 1,
                    "predecessors": [{"id": 1, "ship_strategy": "FORWARD", "side": "second"}],
                },
                {
                    "id": 4,
                    "type": "Sink: Writer",
                    "pact": "Operator",
                    "contents": "Sink: Writer",
                    "parallelism": 1,
                    "predecessors": [{"id": 2, "ship_strategy": "FORWARD", "side": "second"}],
                },
                {
                    "id": 6,
                    "type": "Sink: Committer",
                    "pact": "Operator",
                    "contents": "Sink: Committer",
                    "parallelism": 1,
                    "predecessors": [{"id": 4, "ship_strategy": "FORWARD", "side": "second"}],
                },
            ]
        }
        self.env.get_execution_plan() == json.dumps(expected_plan)

    def test_map_bad_module(self) -> None:

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
            function="sentry_streams.hello.EventsPipelineMapFunction.simple_map",
        )

        _ = KafkaSink(
            name="kafkasink",
            ctx=pipeline,
            inputs=[map],
            logical_topic="transformed-events",
        )

        with self.assertRaises(ImportError):
            iterate_edges(pipeline, self.translator)
