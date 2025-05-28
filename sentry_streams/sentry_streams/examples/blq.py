from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    should_send_to_blq,
)
from sentry_streams.pipeline import segment, streaming_source
from sentry_streams.pipeline.chain import Parser, Serializer, StreamSink

storage_branch = (
    segment(name="recent", msg_type=IngestMetric)
    .apply("serializer1", Serializer())
    .broadcast(
        "send_message_to_DBs",
        routes=[
            segment("sbc", msg_type=IngestMetric).sink(
                "kafkasink", StreamSink(stream_name="transformed-events")
            ),
            segment("clickhouse", msg_type=IngestMetric).sink(
                "kafkasink2", StreamSink(stream_name="transformed-events-2")
            ),
        ],
    )
)

save_delayed_message = (
    segment(name="delayed", msg_type=IngestMetric)
    .apply("serializer2", Serializer())
    .sink(
        "kafkasink3",
        StreamSink(stream_name="transformed-events-3"),
    )
)

pipeline = (
    streaming_source(
        name="ingest",
        stream_name="ingest-metrics",
    )
    .apply("parser", Parser(msg_type=IngestMetric))
    .route(
        "blq_router",
        routing_function=should_send_to_blq,
        routes={
            DownstreamBranch.RECENT: storage_branch,
            DownstreamBranch.DELAYED: save_delayed_message,
        },
    )
)
