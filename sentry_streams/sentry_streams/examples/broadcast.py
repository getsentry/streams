from sentry_streams.examples.broadcast_fn import BroadcastFunctions
from sentry_streams.pipeline import streaming_source
from sentry_streams.pipeline.chain import ExtensibleChain, Map, StreamSink
from sentry_streams.pipeline.message import Message

# Create the source stream
source = streaming_source("myinput", stream_name="events")

# Apply the no-op map transformation
mapped: ExtensibleChain[Message[str]] = source.apply(
    "no_op_map",
    Map(
        function=BroadcastFunctions.no_op_map,
    ),
)

# Broadcast into two branches
hello_branch = mapped.apply(
    "hello_map",
    Map(
        function=BroadcastFunctions.hello_map,
    ),
).sink(
    "hello_sink",
    StreamSink(
        stream_name="transformed-events",
    ),
)

goodbye_branch = mapped.apply(
    "goodbye_map",
    Map(
        function=BroadcastFunctions.goodbye_map,
    ),
).sink(
    "goodbye_sink",
    StreamSink(
        stream_name="transformed-events-2",
    ),
)
