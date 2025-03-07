import pytest

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
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )

    source_2 = KafkaSource(
        name="myinput2",
        ctx=pipeline,
        logical_topic="anotehr-logical-events",
    )

    filter = Filter(
        name="myfilter",
        ctx=pipeline,
        inputs=[source, source_2],
        function=simple_filter,
    )

    _ = Filter(
        name="myfilter2",
        ctx=pipeline,
        inputs=[filter],
        function=simple_filter,
    )

    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[filter],
        function=simple_map,
    )

    map_2 = Map(
        name="mymap2",
        ctx=pipeline,
        inputs=[filter],
        function=simple_map,
    )

    KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map, map_2],
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
    assert pipeline.outgoing_edges["myfilter"] == ["myfilter2", "mymap", "mymap2"]
    assert pipeline.incoming_edges["myfilter"] == ["myinput", "myinput2"]


def test_register_source(pipeline: Pipeline) -> None:
    assert {pipeline.sources[0].name, pipeline.sources[1].name} == {"myinput", "myinput2"}
