from sentry_streams.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)
from sentry_streams.user_functions.sample_filter import EventsPiplineFilterFunctions

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
    function=EventsPiplineFilterFunctions.simple_filter,
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[filter],
    function="sentry_streams.sample_functions.EventsPipelineFunctions.simple_map",
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map],
    logical_topic="transformed-events",
)
