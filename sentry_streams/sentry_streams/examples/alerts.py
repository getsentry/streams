from sentry_kafka_schemas.schema_types.events_v1 import InsertEvent

from sentry_streams.examples.events import (
    AlertsBuffer,
    GroupByAlertID,
    build_alert_json,
    materialize_alerts,
)
from sentry_streams.pipeline import (
    FlatMap,
    Map,
    Parser,
    Reducer,
    streaming_source,
)
from sentry_streams.pipeline.chain import StreamSink
from sentry_streams.pipeline.window import TumblingWindow

pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("parser", Parser(msg_type=InsertEvent))
    # We add a FlatMap so that we can take a stream of events (as above)
    # And then materialize (potentially multiple) time series data points per
    # event. A time series point is materialized per alert rule that the event
    # matches to. For example, if event A has 3 different alerts configured for it,
    # this will materialize 3 times series points for A.
    .apply("myflatmap", FlatMap(function=materialize_alerts))
    # Actually aggregates all the time series data points for each
    # alert rule registered (alert ID). Returns an aggregate value
    # for each window.
    .apply(
        "myreduce",
        Reducer(
            window=TumblingWindow(window_size=3),
            aggregate_func=AlertsBuffer,
            group_by_key=GroupByAlertID(),
        ),
    )
    .apply("map_str", Map(function=build_alert_json))
    .sink("kafkasink", StreamSink(stream_name="transformed-events"))
)
