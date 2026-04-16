from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    streaming_source,
)

pipeline = streaming_source(name="myinput", stream_name="snuba-items").sink(
    DevNullSink(name="devnull")
)
