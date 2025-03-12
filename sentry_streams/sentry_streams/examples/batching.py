from sentry_streams.examples.batch_builder import build_message_str
from sentry_streams.pipeline.pipeline import (
    Batch,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Unbatch,
)

pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

# A sample window.
# Windows are assigned 4 elements.
# reduce_window = TumblingWindow(window_size=4)

reduce: Batch[int, str] = Batch(name="mybatch", ctx=pipeline, inputs=[source], batch_size=5)

unbatch: Unbatch[str] = Unbatch(name="myunbatch", ctx=pipeline, inputs=[reduce])

map = Map(name="mymap", ctx=pipeline, inputs=[unbatch], function=build_message_str)

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map],
    logical_topic="transformed-events",
)
