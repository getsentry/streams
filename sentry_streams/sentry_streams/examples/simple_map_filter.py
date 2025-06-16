from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import transform_msg
from sentry_streams.pipeline import Filter, Map, Parser, Serializer, streaming_source
from sentry_streams.pipeline.chain import StreamSink
from sentry_streams.pipeline.message import Message


def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")


# A pipline with a few transformations
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="ingest-metrics",
    )
    .apply(
        "parser",
        Parser(
            msg_type=IngestMetric,
        ),
    )
    .apply("filter", Filter(function=filter_events))
    .apply("transform", Map(function=transform_msg))
    .apply("serializer", Serializer())
    .sink("mysink", StreamSink(stream_name="transformed-events"))
)
