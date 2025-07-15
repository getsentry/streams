from sentry_streams.examples.broadcast_fn import BroadcastFunctions
from sentry_streams.pipeline.pipeline import (
    Branch,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)

pipeline = Pipeline(
    StreamSource(
        name="myinput",
        stream_name="events",
    )
).apply(
    Map(
        name="no_op_map",
        function=BroadcastFunctions.no_op_map,
    )
)

hello_branch: Pipeline[str] = (
    Pipeline(Branch[str]("hello_branch"))
    .apply(
        Map(
            name="hello_map",
            function=BroadcastFunctions.hello_map,
        )
    )
    .sink(
        StreamSink(
            name="hello_sink",
            stream_name="transformed-events",
        )
    )
)

goodbye_branch: Pipeline[str] = (
    Pipeline(Branch[str]("goodbye_branch"))
    .apply(
        Map(
            name="goodbye_map",
            function=BroadcastFunctions.goodbye_map,
        )
    )
    .sink(
        StreamSink(
            name="goodbye_sink",
            stream_name="transformed-events-2",
        )
    )
)

pipeline = pipeline.broadcast(
    "broadcast",
    routes=[hello_branch, goodbye_branch],
)
