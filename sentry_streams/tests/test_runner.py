from enum import Enum
from typing import Any

import pytest

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import PipelineConfig, RuntimeTranslator
from sentry_streams.dummy.dummy_adapter import DummyAdapter
from sentry_streams.pipeline import Filter, Map, branch, streaming_source
from sentry_streams.pipeline.pipeline import (
    Pipeline,
)
from sentry_streams.runner import iterate_edges


class RouterBranch(Enum):
    BRANCH1 = "branch1"
    BRANCH2 = "branch2"


@pytest.fixture
def create_pipeline() -> Pipeline[bytes]:
    broadcast_branch_1 = (
        branch("branch1")
        .apply(Map("map2", function=lambda x: x.payload))
        .route(
            "router1",
            routing_function=lambda x: RouterBranch.BRANCH1.value,
            routing_table={
                RouterBranch.BRANCH1.value: branch("map4_segment").apply(
                    Map("map4", function=lambda x: x.payload)
                ),
                RouterBranch.BRANCH2.value: branch("map5_segment").apply(
                    Map("map5", function=lambda x: x.payload)
                ),
            },
        )
    )
    broadcast_branch_2 = branch("branch2").apply(Map("map3", function=lambda x: x.payload))

    test_pipeline = (
        streaming_source("source1", stream_name="foo")
        .apply(Map("map1", function=lambda x: x.payload))
        .apply(Filter("filter1", function=lambda x: True))
        .broadcast(
            "broadcast_to_maps",
            routes=[
                broadcast_branch_1,
                broadcast_branch_2,
            ],
        )
    )

    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline[bytes]) -> None:
    dummy_config: PipelineConfig = {}
    runtime: DummyAdapter[Any, Any] = load_adapter("dummy", dummy_config, None)  # type: ignore
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(runtime)
    iterate_edges(create_pipeline, translator)
    assert runtime.input_streams == [
        "source1",
        "map1",
        "filter1",
        "broadcast_to_maps",
        "map2",
        "map3",
        "router1",
        "map4",
        "map5",
    ]
    assert runtime.branches == [
        "branch1",
        "branch2",
        "branch1",
        "branch2",
        "map4_segment",
        "map5_segment",
    ]
