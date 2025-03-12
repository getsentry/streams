from sentry_streams.pipeline.pipeline import (
    Batch,
    KafkaSink,
    KafkaSource,
    Pipeline,
    Unbatch,
)

pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

# User simply provides the batch size
reduce: Batch[int, str] = Batch(name="mybatch", ctx=pipeline, inputs=[source], batch_size=5)

unbatch: Unbatch[str] = Unbatch(name="myunbatch", ctx=pipeline, inputs=[reduce])

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[unbatch],
    logical_topic="transformed-events",
)
