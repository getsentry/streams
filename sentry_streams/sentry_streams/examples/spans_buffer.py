from datetime import timedelta

from sentry_streams.examples.spans import SpansBuffer, build_segment_json, build_span
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)
from sentry_streams.pipeline.window import TumblingWindow

pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[source],
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
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=SpansBuffer,
)

map_str = Map(
    name="map_str",
    ctx=pipeline,
    inputs=[reduce],
    function=build_segment_json,
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map_str],
    logical_topic="transformed-events",
)
