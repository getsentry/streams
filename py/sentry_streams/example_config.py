from sentry_streams.sinks import Pipeline, Printer, RawKafkaSource

# pipeline: special name
pipeline = Pipeline()

source = RawKafkaSource(
    name='myinput',
    ctx=pipeline,
    logical_topic='logical-events'
)

Printer(
    name='myprinter',
    ctx=pipeline,
    inputs=[source],
)
