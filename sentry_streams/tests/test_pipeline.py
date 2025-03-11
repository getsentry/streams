import pytest

from sentry_streams.examples.broadcast import pipeline as broadcast_pipeline
from sentry_streams.pipeline.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Step,
)


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = Pipeline()
    source = KafkaSource(
        name="source",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    source2 = KafkaSource(
        name="source2",
        ctx=pipeline,
        logical_topic="anotehr-logical-events",
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

    KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map, map2],
        logical_topic="transformed-events",
    )
    return pipeline


def simple_filter(value: str) -> bool:
    # does nothing because it's not needed for tests
    return True


def simple_map(value: str) -> str:
    # does nothing because it's not needed for tests
    return "nothing"


def test_register_step(pipeline: Pipeline) -> None:
    step = Step("new_step", pipeline)
    assert "new_step" in pipeline.steps
    assert pipeline.steps["new_step"] == step


def test_register_edge(pipeline: Pipeline) -> None:
    # when there is only one step going to the next step
    assert pipeline.incoming_edges["map"] == ["filter"]
    assert pipeline.outgoing_edges["map2"] == ["kafkasink"]
    # when one step fans out to multiple steps
    assert pipeline.incoming_edges["map2"] == ["filter", "map"]
    assert pipeline.outgoing_edges["filter"] == ["filter2", "map", "map2"]
    # when multiple steps fan into one step
    assert pipeline.incoming_edges["filter"] == ["source", "source2"]
    assert pipeline.outgoing_edges["filter"] == ["filter2", "map", "map2"]


def test_broadcast_branches() -> None:
    assert broadcast_pipeline.outgoing_edges["no_op_map"] == ["hello_map", "goodbye_map"]
    assert broadcast_pipeline.incoming_edges["hello_map"] == ["no_op_map"]
    assert broadcast_pipeline.incoming_edges["goodbye_map"] == ["no_op_map"]


def test_register_source(pipeline: Pipeline) -> None:
    assert {pipeline.sources[0].name, pipeline.sources[1].name} == {"source", "source2"}
