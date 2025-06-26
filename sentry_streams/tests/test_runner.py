from enum import Enum
from typing import Any

import pytest
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import PipelineConfig, RuntimeTranslator
from sentry_streams.dummy.dummy_adapter import DummyAdapter
from sentry_streams.pipeline.chain import Filter, Map, segment, streaming_source
from sentry_streams.pipeline.pipeline import (
    Pipeline,
)
from sentry_streams.runner import iterate_edges


class RouterBranch(Enum):
    BRANCH1 = "branch1"
    BRANCH2 = "branch2"


@pytest.fixture
def create_pipeline() -> Pipeline:
    broadcast_branch_1 = (
        segment("branch1", IngestMetric)
        .apply_step("map2", Map(function=lambda x: x))
        .route(
            "router1",
            routing_function=lambda x: RouterBranch.BRANCH1,
            routes={
                RouterBranch.BRANCH1: segment("map4_segment", IngestMetric).apply_step(
                    "map4", Map(function=lambda x: x)
                ),
                RouterBranch.BRANCH2: segment("map5_segment", IngestMetric).apply_step(
                    "map5", Map(function=lambda x: x)
                ),
            },
        )
    )
    broadcast_branch_2 = segment("branch2", IngestMetric).apply_step(
        "map3", Map(function=lambda x: x)
    )

    test_pipeline = (
        streaming_source("source1", stream_name="foo")
        .apply_step("map1", Map(function=lambda x: x))
        .apply_step("filter1", Filter(function=lambda x: True))
        .broadcast(
            "broadcast_to_maps",
            routes=[
                broadcast_branch_1,
                broadcast_branch_2,
            ],
        )
    )

    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline) -> None:
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
