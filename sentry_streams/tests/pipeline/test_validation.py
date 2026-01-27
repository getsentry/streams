from typing import Any

import pytest

from sentry_streams.pipeline.exception import InvalidPipelineError
from sentry_streams.pipeline.pipeline import (
    Map,
    StreamSink,
    branch,
    streaming_source,
)
from sentry_streams.pipeline.validation import validate_all_branches_have_sinks


def test_valid_pipeline_with_router() -> None:
    """Test that a pipeline with router and all branches having sinks passes validation."""

    def routing_func(msg: Any) -> str:
        return "route1"

    pipeline = (
        streaming_source("myinput", "events")
        .apply(Map("transform1", lambda msg: msg))
        .route(
            "route_to_one",
            routing_function=routing_func,
            routing_table={
                "route1": branch("route1")
                .apply(Map("transform2", lambda msg: msg))
                .sink(StreamSink("myoutput1", stream_name="transformed-events-2")),
                "route2": branch("route2")
                .apply(Map("transform3", lambda msg: msg))
                .sink(StreamSink("myoutput2", stream_name="transformed-events-3")),
            },
        )
    )

    # Should not raise
    validate_all_branches_have_sinks(pipeline)


def test_invalid_pipeline_with_router_missing_sink() -> None:
    """Test that a pipeline with router where one branch is missing a sink fails validation."""

    def routing_func(msg: Any) -> str:
        return "route1"

    pipeline = (
        streaming_source("myinput", "events")
        .apply(Map("transform1", lambda msg: msg))
        .route(
            "route_to_one",
            routing_function=routing_func,
            routing_table={
                "route1": branch("route1")
                .apply(Map("transform2", lambda msg: msg))
                .sink(StreamSink("myoutput1", stream_name="transformed-events-2")),
                "route2": branch("route2").apply(Map("transform3", lambda msg: msg)),
                # Missing sink on route2
            },
        )
    )

    with pytest.raises(InvalidPipelineError) as exc_info:
        validate_all_branches_have_sinks(pipeline)

    assert "transform3" in str(exc_info.value)
    assert "must terminate with a Sink step" in str(exc_info.value)
