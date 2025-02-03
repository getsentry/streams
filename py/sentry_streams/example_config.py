from sentry_streams.primitives import Pipeline, RawKafkaSink, RawKafkaSource

# pipeline: special name
pipeline = Pipeline()

source = RawKafkaSource(
    name="myinput", ctx=pipeline, logical_topic="logical-events", step_type="source"
)

sink = RawKafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[source],
    logical_topic="transformed-events",
    step_type="sink",
)
