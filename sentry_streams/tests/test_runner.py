from typing import Any

import pytest

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import PipelineConfig, RuntimeTranslator
from sentry_streams.dummy.dummy_adapter import DummyAdapter
from sentry_streams.pipeline.pipeline import (
    Branch,
    Filter,
    KafkaSource,
    Map,
    Pipeline,
    Router,
)
from sentry_streams.runner import iterate_edges


@pytest.fixture
def create_pipeline() -> Pipeline:
    test_pipeline = Pipeline()
    source1 = KafkaSource(
        name="source1",
        ctx=test_pipeline,
        logical_topic="foo",
    )
    map1 = Map(
        name="map1",
        inputs=[source1],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    filter1 = Filter(
        name="filter1",
        inputs=[map1],
        ctx=test_pipeline,
        function=lambda x: True,
    )
    map = Map(
        name="map2",
        inputs=[filter1],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    _ = Map(
        name="map3",
        inputs=[filter1],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    router = Router(
        name="router1",
        ctx=test_pipeline,
        inputs=[map],
        routing_table={
            "branch1": Branch(name="branch1", ctx=test_pipeline),
            "branch2": Branch(name="branch2", ctx=test_pipeline),
        },
        routing_function=lambda x: "branch1",
    )
    _ = Map(
        name="map4",
        ctx=test_pipeline,
        inputs=[router.routing_table["branch1"]],
        function=lambda x: x,
    )
    _ = Map(
        name="map5",
        ctx=test_pipeline,
        inputs=[router.routing_table["branch2"]],
        function=lambda x: x,
    )

    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline) -> None:
    dummy_config: PipelineConfig = {}
    runtime: DummyAdapter[Any, Any] = load_adapter("dummy", dummy_config)  # type: ignore
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(runtime)
    iterate_edges(create_pipeline, translator)
    assert runtime.input_streams == {
        "source1": [],
        "map1": ["source1"],
        "filter1": ["source1", "map1"],
        "map2": ["source1", "map1", "filter1"],
        "map3": ["source1", "map1", "filter1"],
        "router1": ["source1", "map1", "filter1", "map2"],
        "branch1": ["source1", "map1", "filter1", "map2", "router1"],
        "branch2": ["source1", "map1", "filter1", "map2", "router1"],
        "map4": ["source1", "map1", "filter1", "map2", "router1", "branch1"],
        "map5": ["source1", "map1", "filter1", "map2", "router1", "branch2"],
    }
