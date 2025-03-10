from sentry_streams.examples.broadcast_fn import BroadcastFunctions
from sentry_streams.pipeline.pipeline import (
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)

pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

map = Map(
    name="no_op_map",
    ctx=pipeline,
    inputs=[source],
    function=BroadcastFunctions.no_op_map,
)

hello_map = Map(
    name="hello_map",
    ctx=pipeline,
    inputs=[map],
    function=BroadcastFunctions.hello_map,
)

goodbye_map = Map(
    name="goodbye_map",
    ctx=pipeline,
    inputs=[map],
    function=BroadcastFunctions.goodbye_map,
)

hello_sink = KafkaSink(
    name="hello_sink",
    ctx=pipeline,
    inputs=[hello_map],
    logical_topic="transformed-events",
)

goodbye_sink = KafkaSink(
    name="goodbye_sink",
    ctx=pipeline,
    inputs=[goodbye_map],
    logical_topic="transformed-events-2",
)
