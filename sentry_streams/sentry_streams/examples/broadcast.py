from sentry_streams.examples.broadcast_fn import BroadcastFunctions
from sentry_streams.pipeline.pipeline import (
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)

pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
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

hello_sink = StreamSink(
    name="hello_sink",
    ctx=pipeline,
    inputs=[hello_map],
    stream_name="transformed-events",
)

goodbye_sink = StreamSink(
    name="goodbye_sink",
    ctx=pipeline,
    inputs=[goodbye_map],
    stream_name="transformed-events-2",
)
