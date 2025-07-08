from datetime import datetime

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.transform_metrics import transform_msg
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Parser,
    Serializer,
    StreamSink,
    streaming_source,
)


def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")


def generate_files() -> str:
    now = datetime.now()
    cur_time = now.strftime("%H:%M:%S")

    return f"file_{cur_time}.txt"


pipeline = streaming_source(name="myinput", stream_name="ingest-metrics")

(
    pipeline.apply(Parser("parser", msg_type=IngestMetric))
    .apply(Filter("filter", function=filter_events))
    .apply(Map("transform", function=transform_msg))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-events"))
)
