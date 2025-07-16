from enum import Enum
from typing import Any, TypeVar, Union, cast
from unittest import mock

import pytest

from sentry_streams.pipeline.pipeline import (
    Batch,
    ComplexStep,
    Filter,
    FlatMap,
    Map,
    Pipeline,
    Reducer,
    StreamSink,
    StreamSource,
    Transform,
    branch,
    make_edge_sets,
    streaming_source,
)
from sentry_streams.pipeline.window import TumblingWindow


def test_sequence() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply(Map("transform1", lambda msg: msg))
        .sink(StreamSink("myoutput", stream_name="transformed-events"))
    )

    assert set(pipeline.steps.keys()) == {"myinput", "transform1", "myoutput"}
    assert cast(StreamSource, pipeline.steps["myinput"]).stream_name == "events"
    assert pipeline.root.name == "myinput"

    assert pipeline.steps["transform1"].name == "transform1"
    assert pipeline.steps["myoutput"].name == "myoutput"

    assert pipeline.incoming_edges["myinput"] == []
    assert pipeline.incoming_edges["transform1"] == ["myinput"]
    assert pipeline.incoming_edges["myoutput"] == ["transform1"]

    assert pipeline.outgoing_edges["myinput"] == ["transform1"]
    assert pipeline.outgoing_edges["transform1"] == ["myoutput"]
    assert pipeline.outgoing_edges["myoutput"] == []


def test_broadcast() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply(Map("transform1", lambda msg: msg))
        .broadcast(
            "route_to_all",
            [
                branch("route1")
                .apply(Map("transform2", lambda msg: msg))
                .sink(StreamSink("myoutput1", stream_name="transformed-events-2")),
                branch("route2")
                .apply(Map("transform3", lambda msg: msg))
                .sink(StreamSink("myoutput2", stream_name="transformed-events-3")),
            ],
        )
    )

    assert set(pipeline.steps.keys()) == {
        "myinput",
        "transform1",
        "route_to_all",
        "route1",
        "route2",
        "transform2",
        "myoutput1",
        "transform3",
        "myoutput2",
    }

    assert make_edge_sets(pipeline.incoming_edges) == {
        "transform1": {"myinput"},
        "route_to_all": {"transform1"},
        "route1": {"route_to_all"},
        "transform2": {"route1"},
        "myoutput1": {"transform2"},
        "route2": {"route_to_all"},
        "transform3": {"route2"},
        "myoutput2": {"transform3"},
    }

    assert make_edge_sets(pipeline.outgoing_edges) == {
        "myinput": {"transform1"},
        "route1": {"transform2"},
        "route2": {"transform3"},
        "route_to_all": {"route1", "route2"},
        "transform1": {"route_to_all"},
        "transform2": {"myoutput1"},
        "transform3": {"myoutput2"},
    }


class Routes(Enum):
    ROUTE1 = "route1"
    ROUTE2 = "route2"


def routing_func(msg: Any) -> str:
    return Routes.ROUTE1.value


def test_router() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply(Map("transform1", lambda msg: msg))
        .route(
            "route_to_one",
            routing_function=routing_func,
            routing_table={
                Routes.ROUTE1.value: branch("route1")
                .apply(Map("transform2", lambda msg: msg))
                .sink(StreamSink("myoutput1", stream_name="transformed-events-2")),
                Routes.ROUTE2.value: branch("route2")
                .apply(Map("transform3", lambda msg: msg))
                .sink(StreamSink("myoutput2", stream_name="transformed-events-3")),
            },
        )
    )

    assert set(pipeline.steps.keys()) == {
        "myinput",
        "transform1",
        "route_to_one",
        "route1",
        "transform2",
        "myoutput1",
        "route2",
        "transform3",
        "myoutput2",
    }

    assert make_edge_sets(pipeline.incoming_edges) == {
        "transform1": {"myinput"},
        "route_to_one": {"transform1"},
        "route1": {"route_to_one"},
        "transform2": {"route1"},
        "myoutput1": {"transform2"},
        "route2": {"route_to_one"},
        "transform3": {"route2"},
        "myoutput2": {"transform3"},
    }

    assert make_edge_sets(pipeline.outgoing_edges) == {
        "myinput": {"transform1"},
        "transform1": {"route_to_one"},
        "route_to_one": {"route1", "route2"},
        "route1": {"transform2"},
        "transform2": {"myoutput1"},
        "route2": {"transform3"},
        "transform3": {"myoutput2"},
    }


TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


@pytest.mark.parametrize(
    "step",
    [
        pytest.param(Map("map_step", lambda msg: msg), id="Create map"),
        pytest.param(Filter("filter_step", lambda msg: True), id="Create filter"),
        pytest.param(FlatMap("flatmap_step", lambda msg: [msg]), id="Create flatMap"),
        pytest.param(
            Reducer(
                "reducer_step",
                window=TumblingWindow(window_size=1),
                aggregate_func=lambda: mock.Mock(),
            ),
            id="Create reducer",
        ),
        pytest.param(Batch("batch_step", batch_size=1), id="Create batch"),
    ],
)
def test_register_steps(step: Union[Transform[Any, Any], ComplexStep[Any, Any]]) -> None:
    name = step.name
    pipeline = Pipeline(StreamSource(name="mysource", stream_name="name"))
    pipeline.apply(step)
    assert pipeline.steps[name] == step
    assert pipeline.steps[name].name == name
    assert pipeline.incoming_edges[name] == ["mysource"]
    assert pipeline.outgoing_edges["mysource"] == [name]
