from typing import Any
from unittest.mock import MagicMock, call

import pytest

from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.pipeline import Filter, KafkaSource, Map, Pipeline
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
    _ = Map(
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
    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline) -> None:
    runtime = MagicMock()
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(runtime)
    iterate_edges(create_pipeline, translator)

    runtime.assert_has_calls(
        [
            call.source(create_pipeline.steps["source1"]),
            call.map(create_pipeline.steps["step1"], runtime.source()),
            call.filter(create_pipeline.steps["step2"], runtime.map()),
            call.map(create_pipeline.steps["step3"], runtime.filter()),
            call.map(create_pipeline.steps["step4"], runtime.filter()),
        ]
    )
