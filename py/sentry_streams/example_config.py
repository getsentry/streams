from sentry_streams.pipeline import KafkaSink, KafkaSource, Map, Pipeline, Reduce
from sentry_streams.user_functions.sample_agg import WordCounter
from sentry_streams.user_functions.sample_group_by import my_group_by
from sentry_streams.user_functions.sample_map import EventsPipelineMapFunction

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

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[map],
    group_by_key=my_group_by,
    aggregate_fn=WordCounter(),
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
