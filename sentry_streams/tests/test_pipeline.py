from typing import Any, Callable, Mapping, Union

import pytest

from sentry_streams.pipeline.pipeline import Batch as BatchStep
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
    TransformStep,
    branch,
    make_edge_sets,
    streaming_source,
)
from sentry_streams.pipeline.window import MeasurementUnit


@pytest.fixture
def pipeline() -> Pipeline:  # type: ignore
    pipeline = (
        streaming_source(name="source", stream_name="events")
        .apply(
            Filter(
                name="filter",
                function=simple_filter,  # type: ignore
            )
        )
        .apply(Filter(name="filter2", function=simple_filter))  # type: ignore
        .apply(Map(name="map", function=simple_map))  # type: ignore
        .apply(Map(name="map2", function=simple_map))  # type: ignore
        .apply(
            Router(
                "router",
                routing_function=simple_router,  # type: ignore
                routing_table={
                    "branch1": Pipeline(Branch("branch1")).sink(
                        StreamSink(name="kafkasink1", stream_name="transformed-events")
                    ),
                    "branch2": Pipeline(Branch("branch2")).sink(
                        StreamSink(name="kafkasink2", stream_name="transformed-events-2")
                    ),
                },
            )
        )
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
    step = Step("new_step")
    pipeline.apply(step)
    assert "new_step" in pipeline.steps
    assert pipeline.steps["new_step"] == step


def test_register_edge(pipeline: Pipeline) -> None:
    # when there is only one step going to the next step
    assert pipeline.incoming_edges["map"] == ["filter2"]
    assert pipeline.outgoing_edges["branch2"] == ["kafkasink2"]
    assert pipeline.incoming_edges["map2"] == ["map"]
    assert pipeline.incoming_edges["filter"] == ["source"]
    assert pipeline.outgoing_edges["filter"] == ["filter2"]
    # when a router splits the stream into multiple branches
    assert pipeline.outgoing_edges["router"] == ["branch1", "branch2"]
    assert pipeline.outgoing_edges["branch1"] == ["kafkasink1"]
    assert pipeline.outgoing_edges["branch2"] == ["kafkasink2"]
    assert pipeline.incoming_edges["branch1"] == ["router"]
    assert pipeline.incoming_edges["branch2"] == ["router"]


def test_register_source(pipeline: Pipeline) -> None:
    assert pipeline.root.name == "source"


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
    pipeline = streaming_source(name="source", stream_name="events")
    step: TransformStep[Any] = TransformStep(
        name="test_resolve_function",
        function=function,
        step_type=StepType.MAP,
    )
    pipeline.apply(step)
    assert step.resolved_function == expected


def test_merge_linear() -> None:
    pipeline1 = streaming_source(name="source", stream_name="logical-events")
    pipeline2 = branch("branch1").apply(Map(name="map", function=simple_map))

    pipeline1._merge(pipeline2, merge_point="source")

    assert set(pipeline1.steps.keys()) == {"source", "map", "branch1"}
    assert pipeline1.outgoing_edges == {
        "source": ["branch1"],
        "branch1": ["map"],
    }
    assert pipeline1.incoming_edges == {
        "map": ["branch1"],
        "branch1": ["source"],
    }


def test_merge_branches() -> None:
    pipeline1 = streaming_source(name="source", stream_name="logical-events")

    pipeline2 = branch(name="branch1").apply(Map(name="map1", function=simple_map))

    pipeline3 = branch(name="branch2").apply(Map(name="map2", function=simple_map))

    pipeline1._merge(pipeline2, merge_point="source")
    pipeline1._merge(pipeline3, merge_point="source")

    assert set(pipeline1.steps.keys()) == {"source", "map1", "map2", "branch1", "branch2"}
    assert make_edge_sets(pipeline1.outgoing_edges) == {
        "source": {"branch1", "branch2"},
        "branch1": {"map1"},
        "branch2": {"map2"},
    }
    assert make_edge_sets(pipeline1.incoming_edges) == {
        "map1": {"branch1"},
        "map2": {"branch2"},
        "branch1": {"source"},
        "branch2": {"source"},
    }


def test_multi_broadcast() -> None:
    pipeline1 = streaming_source(name="source", stream_name="logical-events")

    pipeline2 = branch(name="pipeline2_start")
    branch1 = Pipeline(Branch("branch1")).apply(Map(name="map1", function=simple_map))
    branch2 = Pipeline(Branch("branch2")).apply(Map(name="map2", function=simple_map))

    pipeline2.apply(
        Broadcast(
            "broadcast1",
            routes=[branch1, branch2],
        )
    )

    pipeline1._merge(pipeline2, merge_point="source")
    assert set(pipeline1.steps.keys()) == {
        "source",
        "map1",
        "map2",
        "broadcast1",
        "branch1",
        "branch2",
        "pipeline2_start",
    }
    assert make_edge_sets(pipeline1.outgoing_edges) == {
        "source": {"pipeline2_start"},
        "pipeline2_start": {"broadcast1"},
        "broadcast1": {"branch1", "branch2"},
        "branch1": {"map1"},
        "branch2": {"map2"},
    }
    assert make_edge_sets(pipeline1.incoming_edges) == {
        "map1": {"branch1"},
        "map2": {"branch2"},
        "branch1": {"broadcast1"},
        "branch2": {"broadcast1"},
        "broadcast1": {"pipeline2_start"},
        "pipeline2_start": {"source"},
    }


@pytest.mark.parametrize(
    "loaded_batch_size, default_batch_size, expected",
    [
        pytest.param({"batch_size": 50}, 100, 50, id="Have both loaded and default values"),
        pytest.param({}, 100, 100, id="Only has default app value"),
    ],
)
def test_batch_step_override_config(
    loaded_batch_size: Mapping[str, int],
    default_batch_size: MeasurementUnit,
    expected: MeasurementUnit,
) -> None:
    pipeline = streaming_source(name="mysource", stream_name="name")

    step: BatchStep = BatchStep(name="test-batch", batch_size=default_batch_size)  # type: ignore
    pipeline.apply(step)

    step.override_config(loaded_config=loaded_batch_size)

    assert step.batch_size == expected
