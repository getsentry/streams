from sentry_streams.examples.word_counter_fn import (
    EventsPipelineFilterFunctions,
    EventsPipelineMapFunction,
    GroupByWord,
    WordCounter,
)
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)
from sentry_streams.pipeline.window import TumblingWindow

# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
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

reduce = Aggregate(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=WordCounter,
    group_by_key=GroupByWord(),
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
