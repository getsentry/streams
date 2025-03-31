from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    json_dump_message,
    should_send_to_blq,
    unpack_kafka_message,
)
from sentry_streams.pipeline import Map, segment, streaming_source

storage_branch = (
    segment(name="recent")
    .apply("dump_msg_recent", Map(json_dump_message))
    .broadcast(
        "Send message to DBs",
        routes=[
            segment("sbc").sink("kafkasink", stream_name="transformed-events"),
            segment("clickhouse").sink("kafkasink2", stream_name="transformed-events-2"),
        ],
    )
)

save_delayed_message = (
    segment(name="delayed")
    .apply("dump_msg_delayed", Map(json_dump_message))
    .sink(
        "kafkasink3",
        stream_name="transformed-events3",
    )
)

pipeline = (
    streaming_source(
        name="ingest",
        stream_name="events",
    )
    .apply("unpack_message", Map(unpack_kafka_message))
    .route(
        "blq_router",
        routing_function=should_send_to_blq,
        routes={
            DownstreamBranch.RECENT: storage_branch,
            DownstreamBranch.DELAYED: save_delayed_message,
        },
    )
)
