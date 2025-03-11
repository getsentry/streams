from datetime import timedelta

from sentry_streams.examples.events import AlertsBuffer, build_alert_json, build_event
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)
from sentry_streams.pipeline.window import SlidingWindow

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
    function=build_event,
)

# Windows are set to be open for 5 seconds
reduce_window = SlidingWindow(window_size=timedelta(seconds=5), window_slide=timedelta(seconds=2))

# TODO: Use a flatMap (yet to be supported)
# to emit both a p95 and count
# for the reduce to be applied on

reduce = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=AlertsBuffer,
)

map_str = Map(
    name="map_str",
    ctx=pipeline,
    inputs=[reduce],
    function=build_alert_json,
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map_str],
    logical_topic="transformed-events",
)
