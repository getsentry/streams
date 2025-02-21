from sentry_streams.pipeline import KafkaSink, KafkaSource, Map, Pipeline, Reduce
from sentry_streams.user_functions.sample_agg import WordCounter
from sentry_streams.user_functions.sample_group_by import GroupByWord
from sentry_streams.user_functions.sample_map import EventsPipelineMapFunction
from sentry_streams.window import CountingTrigger, TumblingCountWindow

# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[source],
    function=EventsPipelineMapFunction.simple_map,
)

# A sample window.
# Windows are assigned 3 elements.
# But aggregation can be triggered as soon as 2 elements are seen.
reduce_window = TumblingCountWindow(trigger=CountingTrigger(2), window_size=3)

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    windowing=reduce_window,
    aggregate_fn=WordCounter(),
    group_by_key=GroupByWord(),
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
