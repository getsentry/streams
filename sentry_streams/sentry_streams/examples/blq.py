from sentry_streams.examples.blq_fn import (
    DownstreamBranch,
    json_dump_message,
    should_send_to_blq,
    unpack_kafka_message,
)
from sentry_streams.pipeline.pipeline import (
    Branch,
    Map,
    Pipeline,
    Router,
    StreamSink,
    StreamSource,
)

# pipeline: special name
pipeline = Pipeline()

source = StreamSource(
    name="ingest",
    ctx=pipeline,
    stream="logical-events",
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

sbc_sink = StreamSink(
    name="sbc_sinkStreamSource",
    ctx=pipeline,
    inputs=[dump_msg_recent],
    stream="transformed-events",
)

clickhouse_sink = StreamSink(
    name="clickhouse_sinkStreamSource",
    ctx=pipeline,
    inputs=[dump_msg_recent],
    stream="transformed-eventStreamSource-2",
)

delayed_msg_sink = StreamSink(
    name="delayed_msg_sinkStreamSource",
    ctx=pipeline,
    inputs=[dump_msg_delayed],
    stream="transformed-eventStreamSourceStreamSource",
)
