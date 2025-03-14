from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    KafkaMessage,
    should_send_to_blq,
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
    function=KafkaMessage.unpack,
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

sbc_sink = KafkaSink(
    name="sbc_sink",
    ctx=pipeline,
    inputs=[router.routing_table[DownstreamBranch.RECENT]],
    logical_topic="transformed-events",
)

clickhouse_sink = KafkaSink(
    name="clickhouse_sink",
    ctx=pipeline,
    inputs=[router.routing_table[DownstreamBranch.RECENT]],
    logical_topic="transformed-events-2",
)

delayed_msg_sink = KafkaSink(
    name="delayed_msg_sink",
    ctx=pipeline,
    inputs=[router.routing_table[DownstreamBranch.DELAYED]],
    logical_topic="transformed-events-3",
)
