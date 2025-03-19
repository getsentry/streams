import json

from sentry_streams.pipeline.batch import unbatch
from sentry_streams.pipeline.function_template import InputType
from sentry_streams.pipeline.pipeline import (
    Batch,
    FlatMap,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)


def build_batch_str(batch: list[InputType]) -> str:

    d = {"batch": batch}

    return json.dumps(d)


def build_message_str(message: str) -> str:

    d = {"message": message}

    return json.dumps(d)


pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

# User simply provides the batch size
reduce: Batch[int, str] = Batch(name="mybatch", ctx=pipeline, inputs=[source], batch_size=5)

flat_map = FlatMap(name="myunbatch", ctx=pipeline, inputs=[reduce], function=unbatch)

map = Map(name="mymap", ctx=pipeline, inputs=[flat_map], function=build_message_str)

# flush the batches to the Sink
sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[map],
    logical_topic="transformed-events",
)
