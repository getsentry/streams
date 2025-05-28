from enum import Enum
from typing import Any, TypeVar, cast
from unittest import mock

import pytest
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.chain import (
    Applier,
    Batch,
    Filter,
    FlatMap,
    Map,
    Reducer,
    StreamSink,
    segment,
    streaming_source,
)
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    StreamSource,
    make_edge_sets,
)
from sentry_streams.pipeline.window import TumblingWindow


def test_sequence() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply("transform1", Map(lambda msg: msg))
        .sink("myoutput", StreamSink(stream_name="transformed-events"))
    )

    assert set(pipeline.steps.keys()) == {"myinput", "transform1", "myoutput"}
    assert cast(StreamSource, pipeline.steps["myinput"]).stream_name == "events"
    assert pipeline.steps["myinput"].ctx == pipeline
    assert set(s.name for s in pipeline.sources) == {"myinput"}

    assert pipeline.steps["transform1"].name == "transform1"
    assert pipeline.steps["transform1"].ctx == pipeline

    assert pipeline.steps["myoutput"].name == "myoutput"
    assert pipeline.steps["myoutput"].ctx == pipeline

    assert pipeline.incoming_edges["myinput"] == []
    assert pipeline.incoming_edges["transform1"] == ["myinput"]
    assert pipeline.incoming_edges["myoutput"] == ["transform1"]

    assert pipeline.outgoing_edges["myinput"] == ["transform1"]
    assert pipeline.outgoing_edges["transform1"] == ["myoutput"]
    assert pipeline.outgoing_edges["myoutput"] == []


def test_broadcast() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply("transform1", Map(lambda msg: msg))
        .broadcast(
            "route_to_all",
            [
                segment(name="route1", msg_type=IngestMetric)
                .apply("transform2", Map(lambda msg: msg))
                .sink("myoutput1", StreamSink(stream_name="transformed-events-2")),
                segment(name="route2", msg_type=IngestMetric)
                .apply("transform3", Map(lambda msg: msg))
                .sink("myoutput2", StreamSink(stream_name="transformed-events-3")),
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


def routing_func(msg: Any) -> Routes:
    return Routes.ROUTE1


def test_router() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply("transform1", Map(lambda msg: msg))
        .route(
            "route_to_one",
            routing_function=routing_func,
            routes={
                Routes.ROUTE1: segment(name="route1", msg_type=IngestMetric)
                .apply("transform2", Map(lambda msg: msg))
                .sink("myoutput1", StreamSink(stream_name="transformed-events-2")),
                Routes.ROUTE2: segment(name="route2", msg_type=IngestMetric)
                .apply("transform3", Map(lambda msg: msg))
                .sink("myoutput2", StreamSink(stream_name="transformed-events-3")),
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
    "applier, name",
    [
        pytest.param(Map(lambda msg: msg), "map_step", id="Create map"),
        pytest.param(Filter(lambda msg: True), "filter_step", id="Create filter"),
        pytest.param(FlatMap(lambda msg: [msg]), "flatmap_step", id="Create flatMap"),
        pytest.param(
            Reducer(
                window=TumblingWindow(window_size=1),
                aggregate_func=lambda: mock.Mock(),
            ),
            "reducer_step",
            id="Create reducer",
        ),
        pytest.param(Batch(batch_size=1), "batch_step", id="Create batch"),
    ],
)
def test_applier_steps(applier: Applier[TIn, TOut], name: str) -> None:
    pipeline = Pipeline()
    source = StreamSource(
        name="mysource",
        ctx=pipeline,
        stream_name="name",
    )
    ret = applier.build_step(name, pipeline, source)
    assert pipeline.steps[name] == ret
    assert pipeline.steps[name].name == name
    assert pipeline.incoming_edges[name] == ["mysource"]
    assert pipeline.outgoing_edges["mysource"] == [name]
