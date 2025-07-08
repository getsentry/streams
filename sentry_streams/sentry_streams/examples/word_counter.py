from sentry_streams.examples.word_counter_helpers import (
    GroupByWord,
    WordCounter,
    simple_filter,
    simple_map,
)
from sentry_streams.pipeline import Filter, Map, Reducer, StreamSink, streaming_source
from sentry_streams.pipeline.window import TumblingWindow

# A sample window.
# Windows are assigned 3 elements.
# TODO: Get the parameters for window in pipeline configuration.
reduce_window = TumblingWindow(window_size=3)

# pipeline: special name
pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply(
        Filter(
            "myfilter",
            function=simple_filter,
        ),
    )
    .apply(
        Map(
            "mymap",
            function=simple_map,
        ),
    )
    .apply(
        Reducer(
            "myreduce",
            window=reduce_window,
            aggregate_func=WordCounter,
            group_by_key=GroupByWord(),
        ),
    )
    .sink(
        StreamSink("kafkasink", stream_name="transformed-events"),
    )
)
