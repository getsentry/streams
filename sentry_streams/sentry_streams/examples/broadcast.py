from sentry_streams.examples.broadcast_fn import BroadcastFunctions
from sentry_streams.pipeline.chain import (
    ExtensibleChain,
    Map,
    StreamSink,
    segment,
    streaming_source,
)
from sentry_streams.pipeline.message import Message

pipeline: ExtensibleChain[Message[str]] = streaming_source(
    name="myinput", stream_name="events"
).apply(
    "no_op_map",
    Map(
        function=BroadcastFunctions.no_op_map,
    ),
)

hello_segment = (
    segment(name="hello_segment", msg_type=str)
    .apply(
        "hello_map",
        Map(
            function=BroadcastFunctions.hello_map,
        ),
    )
    .sink(
        "hello_sink",
        StreamSink(
            stream_name="transformed-events",
        ),
    )
)

goodbye_segment = (
    segment(name="goodbye_segment", msg_type=str)
    .apply(
        "goodbye_map",
        Map(
            function=BroadcastFunctions.goodbye_map,
        ),
    )
    .sink(
        "goodbye_sink",
        StreamSink(
            stream_name="transformed-events-2",
        ),
    )
)

pipeline.broadcast(name="hello_broadcast", routes=[hello_segment, goodbye_segment])
