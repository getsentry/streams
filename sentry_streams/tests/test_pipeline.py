from sentry_streams.examples.broadcast import pipeline as broadcast_pipeline
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


def test_broadcast_branches() -> None:
    assert broadcast_pipeline.outgoing_edges["mymap"] == ["hello_map", "goodbye_map"]
    assert broadcast_pipeline.incoming_edges["hello_map"] == ["mymap"]
    assert broadcast_pipeline.incoming_edges["goodbye_map"] == ["mymap"]
