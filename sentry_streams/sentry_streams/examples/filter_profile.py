from sentry_streams.pipeline.pipeline import (
    Batch,
    DevNullSink,
    streaming_source,
)

pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    .apply(Batch(name="mybatch", batch_size=45000))
    .sink(DevNullSink(name="devnull"))
)
