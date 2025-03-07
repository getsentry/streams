from sentry_streams.examples.word_counter import pipeline, source
from sentry_streams.pipeline.pipeline import Step


def test_register_step() -> None:
    step = Step("new_step", pipeline)
    assert "new_step" in pipeline.steps
    assert pipeline.steps["new_step"] == step


def test_register_edge() -> None:
    assert pipeline.outgoing_edges["myfilter"] == ["mymap"]
    assert pipeline.incoming_edges["mymap"] == ["myfilter"]


def test_register_source() -> None:
    assert source in pipeline.sources
