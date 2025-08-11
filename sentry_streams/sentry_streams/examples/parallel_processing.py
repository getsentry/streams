from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import transform_msg
from sentry_streams.pipeline import Map, Parser, Serializer, streaming_source
from sentry_streams.pipeline.pipeline import StreamSink

# A pipeline that transforms messages in parallel
# There are three sequential transformations: parse, transform, serialize.
# They are chained together into one segment that is executed in
# parallel in multiple processes.
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="ingest-metrics",
    )
    .apply(
        Parser[IngestMetric](
            "parser",
        ),
    )
    .apply(Map("transform", function=transform_msg))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-events"))
)
