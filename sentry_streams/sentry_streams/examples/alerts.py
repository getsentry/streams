from sentry_streams.examples.events import (
    AlertsBuffer,
    CountAlertData,
    GroupByAlertID,
    TimeSeriesDataPoint,
    build_alert_json,
    build_event,
    materialize_alerts,
    p95AlertData,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    FlatMap,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import TumblingWindow

pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
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
reduce: Aggregate[int, TimeSeriesDataPoint, p95AlertData | CountAlertData] = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[flat_map],
    window=reduce_window,
    aggregate_func=AlertsBuffer,
    group_by_key=GroupByAlertID(),
)

map_str = Map(
    name="map_str",
    ctx=pipeline,
    inputs=[reduce],
    function=build_alert_json,
)

sink = StreamSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map_str],
    stream_name="transformed-events",
)
