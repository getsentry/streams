from sentry_streams.examples.transform_metrics import (
    fast_filter_events,
)
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Filter,
    HeaderIntFilter,
    streaming_source,
)

pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    .apply(HeaderIntFilter("header_filter", header_name="item_type", value=1))
    .sink(DevNullSink(name="devnull"))
)
