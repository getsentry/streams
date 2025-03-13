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

flat_map = FlatMap(name="myflatmap", ctx=pipeline, inputs=[map], function=materialize_alerts)

reduce_window = TumblingWindow(window_size=3)

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
