from sentry_streams.sinks import Pipeline, RawKafkaSink, RawKafkaSource

# pipeline: special name
pipeline = Pipeline()

source = RawKafkaSource(name="myinput", ctx=pipeline, logical_topic="logical-events")

sink = RawKafkaSink(
    name="kafkasink", ctx=pipeline, inputs=[source], logical_topic="transformed-events"
)

# Printer(
#     name='myprinter',
#     ctx=pipeline,
#     inputs=[source],
# )
