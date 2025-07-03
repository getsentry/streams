from datetime import timedelta

from sentry_streams.examples.spans import SpansBuffer, build_segment_json, build_span
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import TumblingWindow

pipeline = Pipeline(
    StreamSource(
        name="myinput",
        stream_name="events",
    )
)


map = Map(
    name="mymap",
    function=build_span,
)

# A sample window.
# Windows are open for 5 seconds max
reduce_window = TumblingWindow(window_size=timedelta(seconds=5))

# TODO: This example effectively needs a Custom Trigger.
# A Segment can be considered ready if a span named "end" arrives
# Use that as a signal to close the window
# Make the trigger and closing windows synonymous, both
# apparent in the API and as part of implementation

reduce = Aggregate(
    name="myreduce",
    window=reduce_window,
    aggregate_func=SpansBuffer,
)

map_str = Map(
    name="map_str",
    function=build_segment_json,
)

sink = StreamSink(
    name="kafkasink",
    stream_name="transformed-events",
)
