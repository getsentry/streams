from sentry_streams.examples.word_counter_helpers import (
    GroupByWord,
    WordCounter,
    simple_filter,
    simple_map,
)
from sentry_streams.pipeline import Filter, Map, Reducer, streaming_source
from sentry_streams.pipeline.chain import StreamSink
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
    .apply_step(
        "myfilter",
        Filter(
            function=simple_filter,
        ),
    )
    .apply_step(
        "mymap",
        Map(
            function=simple_map,
        ),
    )
    .apply_step(
        "myreduce",
        Reducer(
            window=reduce_window,
            aggregate_func=WordCounter,
            group_by_key=GroupByWord(),
        ),
    )
    .add_sink(
        "kafkasink",
        StreamSink(
            stream_name="transformed-events",
        ),
    )
)
