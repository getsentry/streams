from datetime import timedelta
from sentry_streams.adapters.arroyo.routes import Route
from typing import Any, Callable, Mapping, Union
from unittest import mock

import pytest
from arroyo.processing.strategies.abstract import ProcessingStrategy

from sentry_streams.adapters.arroyo.reduce import build_arroyo_windowed_reduce
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import Batch as BatchStep
from sentry_streams.pipeline.pipeline import (
    Branch,
    Filter,
    Map,
    Pipeline,
    StepType,
    StreamSink,
    TransformStep,
    branch,
    make_edge_sets,
    streaming_source,
)
from sentry_streams.pipeline.window import MeasurementUnit, SlidingWindow


@pytest.fixture
def pipeline() -> Pipeline[str]:
    pipeline: Pipeline[str] = (
        streaming_source(name="source", stream_name="events")
        .apply(
            Filter[bytes](
                name="filter",
                function=simple_filter,
            )
        )
        .apply(Filter[bytes](name="filter2", function=simple_filter))
        .apply(Map[bytes, str](name="map", function=simple_map))
        .apply(Map[str, str](name="map2", function=simple_map_str))
        .route(
            "router",
            routing_function=simple_router,
            routing_table={
                "branch1": Pipeline(Branch[str]("branch1")).sink(
                    StreamSink[str](name="kafkasink1", stream_name="transformed-events")
                ),
                "branch2": Pipeline(Branch[str]("branch2")).sink(
                    StreamSink[str](name="kafkasink2", stream_name="transformed-events-2")
                ),
            },
        )
    )

    return pipeline


def simple_filter(value: Message[bytes]) -> bool:
    # does nothing because it's not needed for tests
    return True


def simple_map(value: Message[bytes]) -> str:
    # does nothing because it's not needed for tests
    return "nothing"


def simple_map_str(value: Message[str]) -> str:
    # does nothing because it's not needed for tests
    return "nothing"


def simple_router(value: Message[str]) -> str:
    # does nothing because it's not needed for tests
    return "branch1"


def test_register_step(pipeline: Pipeline[str]) -> None:
    # Create a new pipeline that isn't closed
    test_pipeline: Pipeline[bytes] = streaming_source(name="source", stream_name="events")
    step = Map[bytes, str](name="new_step", function=lambda msg: "test")
    test_pipeline.apply(step)
    assert "new_step" in test_pipeline.steps
    assert test_pipeline.steps["new_step"] == step


def test_register_edge(pipeline: Pipeline[str]) -> None:
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


def test_register_source(pipeline: Pipeline[str]) -> None:
    assert pipeline.root.name == "source"


class ExampleClass:
    def example_func(self, value: Message[Any]) -> str:
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
    pipeline: Pipeline[bytes] = streaming_source(name="source", stream_name="events")
    step: TransformStep[Any, str] = TransformStep(
        name="test_resolve_function",
        function=function,
        step_type=StepType.MAP,
    )
    pipeline.apply(step)
    assert step.resolved_function == expected


def test_merge_linear() -> None:
    pipeline1: Pipeline[bytes] = streaming_source(name="source", stream_name="logical-events")
    pipeline2: Pipeline[bytes] = branch("branch1").apply(
        Map[Any, bytes](name="map", function=lambda msg: b"test")
    )

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
    pipeline1: Pipeline[bytes] = streaming_source(name="source", stream_name="logical-events")

    pipeline2: Pipeline[bytes] = branch(name="branch1").apply(
        Map[Any, bytes](name="map1", function=lambda msg: b"test1")
    )

    pipeline3: Pipeline[bytes] = branch(name="branch2").apply(
        Map[Any, bytes](name="map2", function=lambda msg: b"test2")
    )

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
    pipeline1: Pipeline[bytes] = streaming_source(name="source", stream_name="logical-events")

    pipeline2: Pipeline[bytes] = branch(name="pipeline2_start")
    branch1: Pipeline[bytes] = Pipeline(Branch[bytes]("branch1")).apply(
        Map[bytes, bytes](name="map1", function=lambda msg: b"test1")
    )
    branch2: Pipeline[bytes] = Pipeline(Branch[bytes]("branch2")).apply(
        Map[bytes, bytes](name="map2", function=lambda msg: b"test2")
    )

    pipeline2.broadcast(
        "broadcast1",
        routes=[branch1, branch2],
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
    pipeline: Pipeline[bytes] = streaming_source(name="mysource", stream_name="name")

    step: BatchStep[MeasurementUnit, bytes] = BatchStep(
        name="test-batch", batch_size=default_batch_size
    )
    pipeline.apply(step)

    step.override_config(loaded_config=loaded_batch_size)

    assert step.batch_size == expected

def test_batch_step_invalid_window_args():
    with pytest.raises(ValueError) as e:
        BatchStep[MeasurementUnit, bytes] = BatchStep(
        name="test-batch", batch_size=10, batch_timedelta=timedelta(seconds=2)
    )
    assert "Only one of 'batch_size' or 'batch_timedelta' may be set, not both." in str(e.value)
    with pytest.raises(ValueError) as e:
        BatchStep[MeasurementUnit, bytes] = BatchStep(
        name="test-batch"
    )
    assert "One of 'batch_size' or 'batch_timedelta' must be set." in str(e.value)

# def test_bat_step_tumbling_window_batch_size():
#     next_step = mock.Mock(spec=ProcessingStrategy)
#     acc = mock.Mock(spec=Accumulator)
#     route = mock.Mock(spec=Route)

#     reduce_window = SlidingWindow(
#         window_size=timedelta(seconds=6), window_slide=timedelta(seconds=0)
#     )

#     with pytest.raises(ValueError):
#         build_arroyo_windowed_reduce(reduce_window, acc, next_step, route)
        
# def test_batch_step_tumbling_window_batch_timedelta():

# def test_batch_step_sliding_window_batch_size():

# def test_batch_step_sliding_window_batch_timedelta():
