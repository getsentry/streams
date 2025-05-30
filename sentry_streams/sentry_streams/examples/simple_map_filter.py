from typing import Any, Mapping

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Filter, Map, Parser, Serializer, streaming_source
from sentry_streams.pipeline.chain import GCSSink
from sentry_streams.pipeline.message import Message


def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")


def transform_msg(msg: Message[IngestMetric]) -> Mapping[str, Any]:
    return {**msg.payload, "transformed": True}


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
    .sink("mysink", GCSSink(bucket="arroyo-artifacts", object_file="uploaded.txt"))
)
