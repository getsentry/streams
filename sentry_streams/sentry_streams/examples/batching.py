from sentry_streams.examples.batch_builder import BatchBuilder
from sentry_streams.pipeline.pipeline import (
    KafkaSink,
    KafkaSource,
    Pipeline,
    Reduce,
)
from sentry_streams.pipeline.window import TumblingWindow

pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

# A sample window.
# Windows are assigned 4 elements.
reduce_window = TumblingWindow(window_size=4)

reduce = Reduce(
    name="myreduce",
    ctx=pipeline,
    inputs=[source],
    windowing=reduce_window,
    aggregate_fn=BatchBuilder,
)

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[reduce],
    logical_topic="transformed-events",
)
