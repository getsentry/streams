from sentry_streams.examples.events import (
    AlertsBuffer,
    GroupByAlertID,
    build_alert_json,
    build_event,
    materialize_alerts,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    FlatMap,
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
    function=build_event,
)

# We add a FlatMap so that we can take a stream of events (as above)
# And then materialize (potentially multiple) time series data points per
# event. A time series point is materialized per alert rule that the event
# matches to. For example, if event A has 3 different alerts configured for it,
# this will materialize 3 times series points for A.
flat_map = FlatMap(name="myflatmap", ctx=pipeline, inputs=[map], function=materialize_alerts)

reduce_window = TumblingWindow(window_size=3)

# Actually aggregates all the time series data points for each
# alert rule registered (alert ID). Returns an aggregate value
# for each window.
reduce = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[flat_map],
    windowing=reduce_window,
    aggregate_fn=AlertsBuffer,
    group_by_key=GroupByAlertID(),
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
