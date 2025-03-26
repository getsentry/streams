from typing import Any, Callable, Union

import pytest

from sentry_streams.pipeline.pipeline import (
    Branch,
    Filter,
    Map,
    Pipeline,
    Router,
    Step,
    StepType,
    StreamSink,
    StreamSource,
    TransformStep,
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
