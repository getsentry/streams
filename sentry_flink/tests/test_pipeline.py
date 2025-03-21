import json
from typing import Any, Generator, MutableMapping

import pytest
from pyflink.datastream import DataStream, DataStreamSink, StreamExecutionEnvironment
from sentry_streams.adapters.stream_adapter import (
    RuntimeTranslator,
    StreamSinkT,
    StreamT,
)
from sentry_streams.examples.word_counter_fn import (
    EventsPipelineFilterFunctions,
    EventsPipelineMapFunction,
    GroupByWord,
    WordCounter,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Branch,
    Filter,
    Map,
    Pipeline,
    Router,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import TumblingWindow
from sentry_streams.runner import iterate_edges

from sentry_flink.flink.flink_adapter import FlinkAdapter


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
            "transformed-events-2": "transformed-events-2",
        },
        "broker": "localhost:9092",
    }
    runtime = FlinkAdapter.build(environment_config)
    translator = RuntimeTranslator(runtime)

    setup_data = (runtime.env, translator)

    yield setup_data


def basic() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:
    pipeline = Pipeline()

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[source],
        stream_name="transformed-events",
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

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function=EventsPipelineMapFunction.simple_map,
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        stream_name="transformed-events",
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

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    filter = Filter(
        name="myfilter",
        ctx=pipeline,
        inputs=[source],
        function=EventsPipelineFilterFunctions.simple_filter,
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[filter],
        stream_name="transformed-events",
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

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function=EventsPipelineMapFunction.simple_map,
    )

    reduce_window = TumblingWindow(window_size=3)

    reduce = Aggregate(
        name="myreduce",
        ctx=pipeline,
        inputs=[map],
        window=reduce_window,
        aggregate_func=WordCounter,
        group_by_key=GroupByWord(),
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[reduce],
        stream_name="transformed-events",
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
                "contents": "Window(CountTumblingWindowAssigner(3), CountTrigger, FlinkAggregate)",
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


def basic_router() -> tuple[Pipeline, MutableMapping[str, list[dict[str, Any]]]]:
    pipeline = Pipeline()

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function=EventsPipelineMapFunction.simple_map,
    )

    router = Router(
        name="myrouter",
        ctx=pipeline,
        inputs=[map],
        routing_table={
            "branch_1": Branch(name="branch_1", ctx=pipeline),
            "branch_2": Branch(name="branch_2", ctx=pipeline),
        },
        routing_function=lambda x: "branch_1" if int(x) % 2 == 0 else "branch_2",
    )

    _ = StreamSink(
        name="kafkasink_1",
        ctx=pipeline,
        inputs=[router.routing_table["branch_1"]],
        stream_name="transformed-events",
    )

    _ = StreamSink(
        name="kafkasink_2",
        ctx=pipeline,
        inputs=[router.routing_table["branch_2"]],
        stream_name="transformed-events-2",
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


@pytest.mark.parametrize(
    "pipeline,expected_plan",
    [basic(), basic_map(), basic_filter(), basic_map_reduce(), basic_router()],
)
def test_pipeline(
    setup_basic_flink_env: tuple[
        StreamExecutionEnvironment, RuntimeTranslator[StreamT, StreamSinkT]
    ],
    pipeline: Pipeline,
    expected_plan: MutableMapping[str, list[dict[str, Any]]],
) -> None:
    env, translator = setup_basic_flink_env

    iterate_edges(pipeline, translator)

    assert json.loads(env.get_execution_plan()) == expected_plan


def bad_import_map() -> Pipeline:
    pipeline = Pipeline()

    source = StreamSource(
        name="myinput",
        ctx=pipeline,
        stream_name="logical-events",
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[source],
        function="sentry_streams.unknown_module.EventsPipelineFunctions.simple_map",
    )

    _ = StreamSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        stream_name="transformed-events",
    )

    return pipeline


def test_import(
    setup_basic_flink_env: tuple[
        StreamExecutionEnvironment, RuntimeTranslator[StreamT, StreamSinkT]
    ],
    pipeline: Pipeline = bad_import_map(),
) -> None:
    _, translator = setup_basic_flink_env

    with pytest.raises(ImportError):
        iterate_edges(pipeline, translator)
