import json
from typing import Any, Generator, MutableMapping

import pytest
from pyflink.datastream import DataStream, DataStreamSink, StreamExecutionEnvironment

from sentry_streams.adapters.stream_adapter import RuntimeTranslator, Stream, StreamSink
from sentry_streams.flink.flink_adapter import FlinkAdapter
from sentry_streams.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Reduce,
)
from sentry_streams.runner import iterate_edges
from sentry_streams.user_functions.sample_agg import (
    WordCounter,
    WordCounterAggregationBackend,
)
from sentry_streams.user_functions.sample_filter import (
    EventsPipelineFilterFunctions,
)
from sentry_streams.user_functions.sample_group_by import GroupByWord
from sentry_streams.user_functions.sample_map import EventsPipelineMapFunction
from sentry_streams.window import TumblingWindow


@pytest.fixture(autouse=True)
def setup_basic_flink_env() -> (
    Generator[
        tuple[StreamExecutionEnvironment, RuntimeTranslator[DataStream, DataStreamSink]], None, None
    ]
):

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": "localhost:9092",
    }
    runtime = FlinkAdapter.build(environment_config)
    translator = RuntimeTranslator(runtime)

    setup_data = (runtime.env, translator)

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
        function=EventsPipelineMapFunction.simple_map,
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


def basic_filter() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:
    pipeline = Pipeline()

    source = KafkaSource(
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    filter = Filter(
        name="myfilter",
        ctx=pipeline,
        inputs=[source],
        function=EventsPipelineFilterFunctions.simple_filter,
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[filter],
        logical_topic="transformed-events",
    )

    expected = {
        "nodes": [
            {
                "id": 14,
                "type": "Source: Custom Source",
                "pact": "Data Source",
                "contents": "Source: Custom Source",
                "parallelism": 1,
            },
            {
                "id": 15,
                "type": "Filter",
                "pact": "Operator",
                "contents": "Filter",
                "parallelism": 1,
                "predecessors": [{"id": 14, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "id": 17,
                "type": "Sink: Writer",
                "pact": "Operator",
                "contents": "Sink: Writer",
                "parallelism": 1,
                "predecessors": [{"id": 15, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "id": 19,
                "type": "Sink: Committer",
                "pact": "Operator",
                "contents": "Sink: Committer",
                "parallelism": 1,
                "predecessors": [{"id": 17, "ship_strategy": "FORWARD", "side": "second"}],
            },
        ]
    }

    return (pipeline, expected)


def basic_map_reduce() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:

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
        function=EventsPipelineMapFunction.simple_map,
    )

    reduce_window = TumblingWindow(window_size=3)
    agg_backend = WordCounterAggregationBackend()

    reduce = Reduce(
        name="myreduce",
        ctx=pipeline,
        inputs=[map],
        windowing=reduce_window,
        aggregate_fn=WordCounter(agg_backend),
        group_by_key=GroupByWord(),
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[reduce],
        logical_topic="transformed-events",
    )

    expected = {
        "nodes": [
            {
                "id": 21,
                "type": "Source: Custom Source",
                "pact": "Data Source",
                "contents": "Source: Custom Source",
                "parallelism": 1,
            },
            {
                "id": 22,
                "type": "Map",
                "pact": "Operator",
                "contents": "Map",
                "parallelism": 1,
                "predecessors": [{"id": 21, "ship_strategy": "FORWARD", "side": "second"}],
            },
            {
                "contents": "Timestamps/Watermarks",
                "id": 23,
                "pact": "Operator",
                "parallelism": 1,
                "predecessors": [
                    {
                        "id": 22,
                        "ship_strategy": "FORWARD",
                        "side": "second",
                    },
                ],
                "type": "Timestamps/Watermarks",
            },
            {
                "contents": "_stream_key_by_map_operator",
                "id": 24,
                "pact": "Operator",
                "parallelism": 1,
                "predecessors": [
                    {
                        "id": 23,
                        "ship_strategy": "FORWARD",
                        "side": "second",
                    },
                ],
                "type": "_stream_key_by_map_operator",
            },
            {
                "contents": "Window(CountTumblingWindowAssigner(3), CountTrigger, "
                "FlinkAggregate)",
                "id": 26,
                "pact": "Operator",
                "parallelism": 1,
                "predecessors": [
                    {
                        "id": 24,
                        "ship_strategy": "HASH",
                        "side": "second",
                    },
                ],
                "type": "CountTumblingWindowAssigner",
            },
            {
                "contents": "Sink: Writer",
                "id": 31,
                "pact": "Operator",
                "parallelism": 1,
                "predecessors": [
                    {
                        "id": 26,
                        "ship_strategy": "FORWARD",
                        "side": "second",
                    },
                ],
                "type": "Sink: Writer",
            },
            {
                "contents": "Sink: Committer",
                "id": 33,
                "pact": "Operator",
                "parallelism": 1,
                "predecessors": [
                    {
                        "id": 31,
                        "ship_strategy": "FORWARD",
                        "side": "second",
                    },
                ],
                "type": "Sink: Committer",
            },
        ]
    }

    return (pipeline, expected)


@pytest.mark.parametrize(
    "pipeline,expected_plan", [basic(), basic_map(), basic_filter(), basic_map_reduce()]
)
def test_pipeline(
    setup_basic_flink_env: tuple[StreamExecutionEnvironment, RuntimeTranslator[Stream, StreamSink]],
    pipeline: Pipeline,
    expected_plan: MutableMapping[str, list[dict[str, Any]]],
) -> None:
    env, translator = setup_basic_flink_env

    iterate_edges(pipeline, translator)

    assert json.loads(env.get_execution_plan()) == expected_plan


def bad_import_map() -> Pipeline:
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
        function="sentry_streams.unknown_module.EventsPipelineFunctions.simple_map",
    )

    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        logical_topic="transformed-events",
    )

    return pipeline


def test_import(
    setup_basic_flink_env: tuple[StreamExecutionEnvironment, RuntimeTranslator[Stream, StreamSink]],
    pipeline: Pipeline = bad_import_map(),
) -> None:
    _, translator = setup_basic_flink_env

    with pytest.raises(ImportError):
        iterate_edges(pipeline, translator)
