from typing import Any
from unittest.mock import ANY, MagicMock, call

import pytest

from sentry_streams.adapters.stream_adapter import RuntimeTranslator
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
    step1 = Map(
        name="step1",
        inputs=[source1],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    step2 = Filter(
        name="step2",
        inputs=[step1],
        ctx=test_pipeline,
        function=lambda x: True,
    )
    map = Map(
        name="step3",
        inputs=[step2],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    _ = Map(
        name="step4",
        inputs=[step2],
        ctx=test_pipeline,
        function=lambda x: x,
    )
    _ = Router(
        name="step5",
        ctx=test_pipeline,
        inputs=[map],
        routing_table={
            "step6": Branch(name="step6", ctx=test_pipeline),
            "step7": Branch(name="step7", ctx=test_pipeline),
        },
        routing_function=lambda x: "branch1",
    )
    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline) -> None:
    runtime = MagicMock()
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(runtime)
    iterate_edges(create_pipeline, translator)

    runtime.assert_has_calls(
        [
            call.source(create_pipeline.steps["source1"]),
            call.map(create_pipeline.steps["step1"], ANY),
            call.filter(create_pipeline.steps["step2"], ANY),
            call.map(create_pipeline.steps["step3"], ANY),
            call.map(create_pipeline.steps["step4"], ANY),
            call.router(create_pipeline.steps["step5"], ANY),
            call.branch(create_pipeline.steps["step6"], ANY),
            call.branch(create_pipeline.steps["step7"], ANY),
        ]
    )
