from unittest.mock import MagicMock, call

import pytest

from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline import Filter, KafkaSource, Map, Pipeline
from sentry_streams.runner import iterate_edges


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = Pipeline()
    source1 = KafkaSource(name="source1", ctx=pipeline, logical_topic="foo")
    step1 = Map(name="step1", inputs=[source1], ctx=pipeline, function=lambda x: x)
    step2 = Filter(name="step2", inputs=[step1], ctx=pipeline, function=lambda x: True)
    _ = Map(name="step3", inputs=[step2], ctx=pipeline, function=lambda x: x)
    _ = Map(name="step4", inputs=[step2], ctx=pipeline, function=lambda x: x)

    return pipeline


def test_iterate_edges(pipeline: Pipeline) -> None:
    runtime = MagicMock()
    translator = RuntimeTranslator(runtime)
    iterate_edges(pipeline, translator)

    runtime.assert_has_calls(
        [
            call.source(pipeline.steps["source1"]),
            call.map(pipeline.steps["step1"], runtime.source()),
            call.filter(pipeline.steps["step2"], runtime.map()),
            call.map(pipeline.steps["step3"], runtime.filter()),
            call.map(pipeline.steps["step4"], runtime.filter()),
        ]
    )
