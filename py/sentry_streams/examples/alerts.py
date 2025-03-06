from datetime import timedelta

from sentry_streams.examples.events import AlertsBuffer, build_alert_json, build_event
from sentry_streams.pipeline import (
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Reduce,
)
from sentry_streams.window import SlidingWindow

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

# TODO: We want to produce two different
# Reduce streams (a fan-out), one for the p95 latencies
# and one for the counts

reduce = Reduce(
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
