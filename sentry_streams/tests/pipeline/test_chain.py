from sentry_streams.pipeline.chain import Chain, streaming_source, segment, Reducer
from sentry_streams.pipeline.chain import Map, Filter, FlatMap, Reducer
from json import loads
from enum import Enum
from typing import Mapping, Any, cast
from sentry_streams.examples.billing_buffer import OutcomesBuffer
from sentry_streams.pipeline.function_template import KVAggregationBackend
from sentry_streams.pipeline.window import TumblingWindow
from sentry_streams.pipeline.pipeline import StreamSource


def test_sequence() -> None:
    pipeline = (
        streaming_source("myinput", "events")
        .apply("transform1", Map(lambda msg: msg))
        .sink("myoutput", "transformed-events")
    )

    assert set(pipeline.steps.keys()) == {"myinput", "transform1", "myoutput"}
    assert cast(StreamSource, pipeline.steps["myinput"]).stream_name == "events"
    assert pipeline.steps["myinput"].ctx == pipeline
    assert set(s.name for s in pipeline.sources) == {"myinput"}

    assert pipeline.steps["transform1"].name == "transform1"
    assert pipeline.steps["transform1"].ctx == pipeline

    assert pipeline.steps["myoutput"].name == "myoutput"
    assert pipeline.steps["myoutput"].ctx == pipeline

    assert pipeline.incoming_edges["myinput"] == []
    assert pipeline.incoming_edges["transform1"] == ["myinput"]
    assert pipeline.incoming_edges["myoutput"] == ["transform1"]

    assert pipeline.outgoing_edges["myinput"] == ["transform1"]
    assert pipeline.outgoing_edges["transform1"] == ["myoutput"]
    assert pipeline.outgoing_edges["myoutput"] == []
