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
        .apply_step("parse_msg", Parser(msg_type=IngestMetric))
        .apply_step("process", Map(do_something))
        .apply_step("serialize", Serializer())
        .add_sink("eventstream", StreamSink(stream_name="events")),
        # Snuba chain to Clickhouse
        streaming_source("snuba", stream_name="ingest-metrics")
        .apply_step("snuba_parse_msg", Parser(msg_type=IngestMetric))
        .apply_step("snuba_serialize", Serializer())
        .add_sink(
            "clickhouse",
            StreamSink(stream_name="someewhere"),
        ),
        # Super Big Consumer chain
        streaming_source("sbc", stream_name="ingest-metrics")
        .apply_step("sbc_parse_msg", Parser(msg_type=IngestMetric))
        .apply_step("sbc_serialize", Serializer())
        .add_sink(
            "sbc_sink",
            StreamSink(stream_name="someewhere"),
        ),
        # Post process chain
        streaming_source("post_process", stream_name="ingest-metrics")
        .apply_step("post_parse_msg", Parser(msg_type=IngestMetric))
        .apply_step("postprocess", Map(do_something))
        .apply_step("postprocess_serialize", Serializer())
        .add_sink(
            "devnull",
            StreamSink(stream_name="someewhereelse"),
        ),
    ]
)
