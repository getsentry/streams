from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Map, multi_chain, streaming_source
from sentry_streams.pipeline.chain import Parser, Serializer
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_parser import json_parser, json_serializer


def do_something(msg: Message[IngestMetric]) -> Message[IngestMetric]:
    # Do something with the message
    return msg


pipeline = multi_chain(
    [
        # Main Ingest chain
        streaming_source("ingest", stream_name="ingest-metrics")
        .apply("parse_msg", Parser(msg_type=IngestMetric, deserializer=json_parser))
        .apply("process", Map(do_something))
        .apply("serialize", Serializer(serializer=json_serializer))
        .sink("eventstream", stream_name="events"),
        # Snuba chain to Clickhouse
        streaming_source("snuba", stream_name="ingest-metrics")
        .apply("snuba_parse_msg", Parser(msg_type=IngestMetric, deserializer=json_parser))
        .apply("snuba_serialize", Serializer(serializer=json_serializer))
        .sink(
            "clickhouse",
            stream_name="someewhere",
        ),
        # Super Big Consumer chain
        streaming_source("sbc", stream_name="ingest-metrics")
        .apply("sbc_parse_msg", Parser(msg_type=IngestMetric, deserializer=json_parser))
        .apply("sbc_serialize", Serializer(serializer=json_serializer))
        .sink(
            "sbc_sink",
            stream_name="someewhere",
        ),
        # Post process chain
        streaming_source("post_process", stream_name="ingest-metrics")
        .apply("post_parse_msg", Parser(msg_type=IngestMetric, deserializer=json_parser))
        .apply("postprocess", Map(do_something))
        .apply("postprocess_serialize", Serializer(serializer=json_serializer))
        .sink(
            "devnull",
            stream_name="someewhereelse",
        ),
    ]
)
