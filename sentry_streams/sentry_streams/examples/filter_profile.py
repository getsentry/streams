# from sentry_streams.examples.transform_metrics import transform_raw
from sentry_streams.pipeline.pipeline import (  # Map,
    DevNullSink,
    streaming_source,
)

pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    # .apply(Map("transform", function=transform_raw))
    .sink(DevNullSink(name="devnull"))
)
