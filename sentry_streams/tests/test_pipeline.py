from typing import Any, Callable, Union

import pytest

from sentry_streams.pipeline.chain import StreamSink as StreamSinkStep
from sentry_streams.pipeline.chain import streaming_source
from sentry_streams.pipeline.pipeline import (
    Branch,
    Broadcast,
    Filter,
    Map,
    Pipeline,
    Router,
    Step,
    StepType,
    StreamSink,
    StreamSource,
    TransformStep,
    make_edge_sets,
)


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = Pipeline()
    source = StreamSource(
        name="source",
        ctx=pipeline,
        stream_name="events",
    )

    source2 = StreamSource(
        name="source2",
        ctx=pipeline,
        stream_name="anotehr-events",
    )

    filter = Filter(
        name="filter",
        ctx=pipeline,
        inputs=[source, source2],
        function=simple_filter,
    )

    _ = Filter(
        name="filter2",
        ctx=pipeline,
        inputs=[filter],
        function=simple_filter,
    )

    map = Map(
        name="map",
        ctx=pipeline,
        inputs=[filter],
        function=simple_map,
    )

    map2 = Map(
        name="map2",
        ctx=pipeline,
        inputs=[filter, map],
        function=simple_map,
    )

    router = Router(
        name="router",
        ctx=pipeline,
        inputs=[map2],
        routing_table={
            "branch1": Branch(name="branch1", ctx=pipeline),
            "branch2": Branch(name="branch2", ctx=pipeline),
        },
        routing_function=simple_router,
    )

    StreamSink(
        name="kafkasink1",
        ctx=pipeline,
        inputs=[router.routing_table["branch1"]],
        stream_name="transformed-events",
    )

    StreamSink(
        name="kafkasink2",
        ctx=pipeline,
        inputs=[router.routing_table["branch2"]],
        stream_name="transformed-events-2",
    )
    return pipeline


def simple_filter(value: str) -> bool:
    # does nothing because it's not needed for tests
    return True


def simple_map(value: str) -> str:
    # does nothing because it's not needed for tests
    return "nothing"


def simple_router(value: str) -> str:
    # does nothing because it's not needed for tests
    return "branch1"


def test_register_step(pipeline: Pipeline) -> None:
    step = Step("new_step", pipeline)
    assert "new_step" in pipeline.steps
    assert pipeline.steps["new_step"] == step


def test_register_edge(pipeline: Pipeline) -> None:
    # when there is only one step going to the next step
    assert pipeline.incoming_edges["map"] == ["filter"]
    assert pipeline.outgoing_edges["branch2"] == ["kafkasink2"]
    # when one step fans out to multiple steps
    assert pipeline.incoming_edges["map2"] == ["filter", "map"]
    assert pipeline.outgoing_edges["filter"] == ["filter2", "map", "map2"]
    # when multiple steps fan into one step
    assert pipeline.incoming_edges["filter"] == ["source", "source2"]
    assert pipeline.outgoing_edges["filter"] == ["filter2", "map", "map2"]
    # when a router splits the stream into multiple branches
    assert pipeline.outgoing_edges["router"] == ["branch1", "branch2"]
    assert pipeline.outgoing_edges["branch1"] == ["kafkasink1"]
    assert pipeline.outgoing_edges["branch2"] == ["kafkasink2"]
    assert pipeline.incoming_edges["branch1"] == ["router"]
    assert pipeline.incoming_edges["branch2"] == ["router"]


def test_register_source(pipeline: Pipeline) -> None:
    assert {pipeline.sources[0].name, pipeline.sources[1].name} == {"source", "source2"}


class ExampleClass:
    def example_func(self, value: str) -> str:
        return "nothing"


@pytest.mark.parametrize(
    "function, expected",
    [
        pytest.param(
            "tests.test_pipeline.ExampleClass.example_func",
            ExampleClass.example_func,
            id="Function is a string of an relative path, referring to a function inside a class",
        ),
        pytest.param(
            "tests.test_pipeline.simple_map",
            simple_map,
            id="Function is a string of an relative path, referring to a function outside of a class",
        ),
        pytest.param(
            ExampleClass.example_func,
            ExampleClass.example_func,
            id="Function is a callable",
        ),
    ],
)
def test_resolve_function(
    function: Union[Callable[..., str], str], expected: Callable[..., str]
) -> None:
    pipeline = Pipeline()
    step: TransformStep[Any] = TransformStep(
        name="test_resolve_function",
        ctx=pipeline,
        inputs=[],
        function=function,
        step_type=StepType.MAP,
    )
    assert step.resolved_function == expected


def test_merge_linear() -> None:
    pipeline1 = Pipeline()
    StreamSource(
        name="source",
        ctx=pipeline1,
        stream_name="logical-events",
    )

    pipeline2 = Pipeline()
    branch = Branch(
        "branch1",
        pipeline2,
    )
    Map(
        name="map",
        ctx=pipeline2,
        inputs=[branch],
        function=simple_map,
    )

    pipeline1.merge(pipeline2, merge_point="source")

    assert set(pipeline1.steps.keys()) == {"source", "map"}
    assert pipeline1.outgoing_edges == {
        "source": ["map"],
    }
    assert pipeline1.incoming_edges == {
        "map": ["source"],
    }


def test_merge_branches() -> None:
    pipeline1 = Pipeline()
    StreamSource(
        name="source",
        ctx=pipeline1,
        stream_name="logical-events",
    )

    pipeline2 = Pipeline()
    branch1 = Branch(
        "branch1",
        pipeline2,
    )
    Map(
        name="map1",
        ctx=pipeline2,
        inputs=[branch1],
        function=simple_map,
    )

    pipeline3 = Pipeline()
    branch2 = Branch(
        "branch2",
        pipeline3,
    )
    Map(
        name="map2",
        ctx=pipeline3,
        inputs=[branch2],
        function=simple_map,
    )

    pipeline1.merge(pipeline2, merge_point="source")
    pipeline1.merge(pipeline3, merge_point="source")

    assert set(pipeline1.steps.keys()) == {"source", "map1", "map2"}
    assert make_edge_sets(pipeline1.outgoing_edges) == {
        "source": {"map1", "map2"},
    }
    assert make_edge_sets(pipeline1.incoming_edges) == {
        "map1": {"source"},
        "map2": {"source"},
    }


def test_multi_broadcast() -> None:
    pipeline1 = Pipeline()
    StreamSource(
        name="source",
        ctx=pipeline1,
        stream_name="logical-events",
    )

    pipeline2 = Pipeline()
    pipeline2_start = Branch(
        "pipeline2_start",
        pipeline2,
    )

    broadcast = Broadcast(
        "broadcast1",
        ctx=pipeline2,
        inputs=[pipeline2_start],
        routes=[
            Branch("branch1", ctx=pipeline2),
            Branch("branch2", ctx=pipeline2),
        ],
    )
    Map(
        name="map1",
        ctx=pipeline2,
        inputs=[broadcast.routes[0]],
        function=simple_map,
    )
    Map(
        name="map2",
        ctx=pipeline2,
        inputs=[broadcast.routes[1]],
        function=simple_map,
    )

    pipeline1.merge(pipeline2, merge_point="source")

    assert set(pipeline1.steps.keys()) == {
        "source",
        "map1",
        "map2",
        "broadcast1",
        "branch1",
        "branch2",
    }
    assert make_edge_sets(pipeline1.outgoing_edges) == {
        "source": {"broadcast1"},
        "broadcast1": {"branch1", "branch2"},
        "branch1": {"map1"},
        "branch2": {"map2"},
    }
    assert make_edge_sets(pipeline1.incoming_edges) == {
        "map1": {"branch1"},
        "map2": {"branch2"},
        "branch1": {"broadcast1"},
        "branch2": {"broadcast1"},
        "broadcast1": {"source"},
    }


def test_add_empty_pipeline_to_empty_pipeline() -> None:
    pipeline1 = Pipeline()
    pipeline2 = Pipeline()

    pipeline1.add(pipeline2)

    assert len(pipeline1.steps) == 0
    assert len(pipeline1.sources) == 0
    assert len(pipeline1.incoming_edges) == 0
    assert len(pipeline1.outgoing_edges) == 0


def test_add_to_empty() -> None:
    pipeline1 = Pipeline()

    pipeline2 = streaming_source("source", "events").sink(
        "sink", StreamSinkStep(stream_name="processed-events")
    )
    pipeline1.add(pipeline2)

    assert len(pipeline1.steps) == 2
    assert len(pipeline1.sources) == 1
    assert pipeline1.sources[0].name == "source"
    assert pipeline1.incoming_edges["sink"] == ["source"]
    assert pipeline1.outgoing_edges["source"] == ["sink"]


def test_add_multi_pipeline() -> None:
    pipeline1 = Pipeline()

    pipeline2 = streaming_source("source1", "events").sink(
        "sink1", StreamSinkStep(stream_name="processed-events")
    )
    pipeline1.add(pipeline2)

    pipeline2 = streaming_source("source2", "events").sink(
        "sink2", StreamSinkStep(stream_name="processed-events")
    )
    pipeline1.add(pipeline2)

    assert len(pipeline1.steps) == 4
    assert len(pipeline1.sources) == 2
    assert {source.name for source in pipeline1.sources} == {"source1", "source2"}
    assert pipeline1.incoming_edges["sink1"] == ["source1"]
    assert pipeline1.incoming_edges["sink2"] == ["source2"]
    assert pipeline1.outgoing_edges["source1"] == ["sink1"]
    assert pipeline1.outgoing_edges["source2"] == ["sink2"]


def test_invalid_add() -> None:
    pipeline1 = Pipeline()

    pipeline2 = streaming_source("source", "events").sink(
        "sink", StreamSinkStep(stream_name="processed-events")
    )
    pipeline1.add(pipeline2)

    with pytest.raises(AssertionError):
        pipeline1.add(pipeline2)
