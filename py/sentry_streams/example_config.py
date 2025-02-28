from sentry_streams.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)
from sentry_streams.user_functions.sample_filter import (
    EventsPipelineFilterFunctions,
    EventsPipelineMapFunctions,
)

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
    function=EventsPipelineMapFunctions.simple_map,
)

branch_1 = Map(
    name="mybranch1",
    ctx=pipeline,
    inputs=[map],
    function=EventsPipelineMapFunctions.simple_map,
)

branch_2 = Map(
    name="mybranch2",
    ctx=pipeline,
    inputs=[map],
    function=EventsPipelineMapFunctions.simple_map,
)

sink_1 = KafkaSink(
    name="kafkasink1",
    ctx=pipeline,
    inputs=[branch_1],
    logical_topic="transformed-events",
)

sink_2 = KafkaSink(
    name="kafkasink2",
    ctx=pipeline,
    inputs=[branch_2],
    logical_topic="transformed-events-2",
)
