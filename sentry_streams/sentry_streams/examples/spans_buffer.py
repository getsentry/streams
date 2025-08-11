from datetime import timedelta

from sentry_kafka_schemas.schema_types.snuba_spans_v1 import SpanEvent

from sentry_streams.examples.span_helpers import SpansBuffer, build_segment_json
from sentry_streams.pipeline import Map, Parser, Reducer, StreamSink, streaming_source
from sentry_streams.pipeline.window import TumblingWindow

# A sample window.
# Windows are open for 5 seconds max
reduce_window = TumblingWindow(window_timedelta=timedelta(seconds=5))

# TODO: This example effectively needs a Custom Trigger.
# A Segment can be considered ready if a span named "end" arrives
# Use that as a signal to close the window
# Make the trigger and closing windows synonymous, both
# apparent in the API and as part of implementation

pipeline = (
    streaming_source(name="myinput", stream_name="events")
    .apply(Parser[SpanEvent]("mymap"))
    .apply(
        Reducer(
            "myreduce",
            window=reduce_window,
            aggregate_func=SpansBuffer,
        ),
    )
    .apply(
        Map(
            "map_str",
            function=build_segment_json,
        ),
    )
    .sink(
        StreamSink(
            "kafkasink",
            stream_name="transformed-events",
        ),
    )
)
