from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    json_dump_message,
    should_send_to_blq,
    unpack_kafka_message,
)
from sentry_streams.pipeline.pipeline import (
    Branch,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Router,
)

# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="ingest",
    ctx=pipeline,
    logical_topic="logical-events",
)

unpack_msg = Map(
    name="unpack_message",
    ctx=pipeline,
    inputs=[source],
    function=unpack_kafka_message,
)

router = Router(
    name="blq_router",
    ctx=pipeline,
    inputs=[unpack_msg],
    routing_table={
        DownstreamBranch.RECENT: Branch(name="recent", ctx=pipeline),
        DownstreamBranch.DELAYED: Branch(name="delayed", ctx=pipeline),
    },
    routing_function=should_send_to_blq,
)

dump_msg_recent = Map(
    name="dump_msg_recent",
    ctx=pipeline,
    inputs=[router.routing_table[DownstreamBranch.RECENT]],
    function=json_dump_message,
)

dump_msg_delayed = Map(
    name="dump_msg_delayed",
    ctx=pipeline,
    inputs=[router.routing_table[DownstreamBranch.DELAYED]],
    function=json_dump_message,
)

sbc_sink = KafkaSink(
    name="sbc_sink",
    ctx=pipeline,
    inputs=[dump_msg_recent],
    logical_topic="transformed-events",
)

clickhouse_sink = KafkaSink(
    name="clickhouse_sink",
    ctx=pipeline,
    inputs=[dump_msg_recent],
    logical_topic="transformed-events-2",
)

delayed_msg_sink = KafkaSink(
    name="delayed_msg_sink",
    ctx=pipeline,
    inputs=[dump_msg_delayed],
    logical_topic="transformed-events-3",
)
