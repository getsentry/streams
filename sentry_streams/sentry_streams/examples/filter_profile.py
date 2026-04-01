from sentry_streams.examples.transform_metrics import (
    fast_filter_events,
)
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Filter,
    streaming_source,
)

pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    .apply(Filter("filter", function=fast_filter_events))
    .sink(DevNullSink(name="devnull"))
)
