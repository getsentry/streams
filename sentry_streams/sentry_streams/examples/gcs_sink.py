from datetime import datetime

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import transform_msg
from sentry_streams.pipeline import Filter, Map, Parser, Serializer, streaming_source
from sentry_streams.pipeline.pipeline import GCSSink
from sentry_streams.pipeline.message import Message


def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")


def generate_files() -> str:
    now = datetime.now()
    cur_time = now.strftime("%H:%M:%S")

    return f"file_{cur_time}.txt"


# A pipline with a few transformations
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="ingest-metrics",
    )
    .apply(
        Parser[IngestMetric]("myparser"),
    )
    .apply(Filter(name="filter", function=filter_events))
    .apply(Map(name="transform", function=transform_msg))
    .apply(Serializer("myserializer"))
    .sink(GCSSink(name="mysink", bucket="arroyo-artifacts", object_generator=generate_files))
)