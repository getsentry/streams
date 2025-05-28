from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Map, multi_chain, streaming_source
from sentry_streams.pipeline.chain import Parser, Serializer, StreamSink
from sentry_streams.pipeline.message import Message


def do_something(msg: Message[IngestMetric]) -> Message[IngestMetric]:
    # Do something with the message
    return msg


pipeline = multi_chain(
    [
        # Main Ingest chain
        streaming_source("ingest", stream_name="ingest-metrics")
        .apply("parse_msg", Parser(msg_type=IngestMetric))
        .apply("process", Map(do_something))
        .apply("serialize", Serializer())
        .sink("eventstream", StreamSink(stream_name="events")),
        # Snuba chain to Clickhouse
        streaming_source("snuba", stream_name="ingest-metrics")
        .apply("snuba_parse_msg", Parser(msg_type=IngestMetric))
        .apply("snuba_serialize", Serializer())
        .sink(
            "clickhouse",
            StreamSink(stream_name="someewhere"),
        ),
        # Super Big Consumer chain
        streaming_source("sbc", stream_name="ingest-metrics")
        .apply("sbc_parse_msg", Parser(msg_type=IngestMetric))
        .apply("sbc_serialize", Serializer())
        .sink(
            "sbc_sink",
            StreamSink(stream_name="someewhere"),
        ),
        # Post process chain
        streaming_source("post_process", stream_name="ingest-metrics")
        .apply("post_parse_msg", Parser(msg_type=IngestMetric))
        .apply("postprocess", Map(do_something))
        .apply("postprocess_serialize", Serializer())
        .sink(
            "devnull",
            StreamSink(stream_name="someewhereelse"),
        ),
    ]
)
