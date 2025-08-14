from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    should_send_to_blq,
)
from sentry_streams.pipeline import branch, streaming_source
from sentry_streams.pipeline.pipeline import Parser, Serializer, StreamSink

storage_branch = (
    branch("recent")
    .apply(Serializer("serializer1"))
    .broadcast(
        "send_message_to_DBs",
        routes=[
            branch("sbc").sink(StreamSink("kafkasink", stream_name="transformed-events")),
            branch("clickhouse").sink(StreamSink("kafkasink2", stream_name="transformed-events-2")),
        ],
    )
)

save_delayed_message = (
    branch("delayed")
    .apply(Serializer("serializer2"))
    .sink(
        StreamSink("kafkasink3", stream_name="transformed-events-3"),
    )
)

pipeline = (
    streaming_source(
        name="ingest",
        stream_name="ingest-metrics",
    )
    .apply(Parser[IngestMetric]("parser"))
    .route(
        "blq_router",
        routing_function=should_send_to_blq,
        routing_table={
            DownstreamBranch.RECENT: storage_branch,  # type: ignore
            DownstreamBranch.DELAYED: save_delayed_message,  # type: ignore
        },
    )
)
