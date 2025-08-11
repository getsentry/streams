from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import filter_events, transform_msg
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Parser,
    Serializer,
    StreamSink,
    streaming_source,
)

pipeline = streaming_source(name="myinput", stream_name="ingest-metrics")

(
    pipeline.apply(Parser[IngestMetric]("parser"))
    .apply(Filter("filter", function=filter_events))
    .apply(Map("transform", function=transform_msg))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-events"))
)
