from sentry_streams.pipeline.pipeline import (
    Batch,
    DevNullSink,
    HeadersFilter,
    streaming_source,
)

pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    .apply(
        HeadersFilter(
            name="spans_filter",
            header_name="item_type",
            value=1,
        )
    )
    .apply(Batch(name="mybatch", batch_size=45000))
    .sink(DevNullSink(name="devnull"))
)
