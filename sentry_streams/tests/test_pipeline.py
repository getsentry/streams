import json

from sentry_streams.pipeline.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Step,
)

pipeline = Pipeline()


def simple_filter(value: str) -> bool:
    d = json.loads(value)
    return True if "name" in d else False


def simple_map(value: str) -> str:
    d = json.loads(value)
    res: str = d.get("name", "no name")

    return "hello " + res


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

filter_2 = Filter(
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

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map, map_2],
    logical_topic="transformed-events",
)


def test_register_step() -> None:
    step = Step("new_step", pipeline)
    assert "new_step" in pipeline.steps
    assert pipeline.steps["new_step"] == step


def test_register_edge() -> None:
    assert pipeline.outgoing_edges["myfilter"] == ["myfilter2", "mymap", "mymap2"]
    assert pipeline.incoming_edges["myfilter"] == ["myinput", "myinput2"]


def test_register_source() -> None:
    assert source in pipeline.sources
