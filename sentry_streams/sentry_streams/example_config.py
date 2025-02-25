from sentry_streams.pipeline import KafkaSink, KafkaSource, Map, Pipeline

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
    function="sentry_streams.sample_function.EventsPipelineMapFunction.simple_map",
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map],
    logical_topic="transformed-events",
)
