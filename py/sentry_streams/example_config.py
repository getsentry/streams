from sentry_streams.pipeline import KafkaSink, KafkaSource, Pipeline

# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[source],
    logical_topic="transformed-events",
)
