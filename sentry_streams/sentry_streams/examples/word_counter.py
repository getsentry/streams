from sentry_streams.examples.word_counter_fn import (
    EventsPipelineFilterFunctions,
    EventsPipelineMapFunction,
    GroupByWord,
    WordCounter,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Filter,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)
from sentry_streams.pipeline.window import TumblingWindow

# pipeline: special name
pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
)

filter = Filter(
    name="myfilter",
    ctx=pipeline,
    inputs=[source],
    function=EventsPipelineFilterFunctions.simple_filter,
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[filter],
    function=EventsPipelineMapFunction.simple_map,
)

# A sample window.
# Windows are assigned 3 elements.
# TODO: Get the parameters for window in pipeline configuration.
reduce_window = TumblingWindow(window_size=3)

reduce: Aggregate[int, tuple[str, int], str] = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    window=reduce_window,
    aggregate_func=WordCounter,
    group_by_key=GroupByWord(),
)

sink = StreamSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    stream_name="transformed-events",
)
