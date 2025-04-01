import json

from sentry_streams.pipeline import Batch, FlatMap, Map, streaming_source
from sentry_streams.pipeline.batch import unbatch
from sentry_streams.pipeline.function_template import InputType


def build_batch_str(batch: list[InputType]) -> str:
    d = {"batch": batch}

    return json.dumps(d)


def build_message_str(message: str) -> str:
    d = {"message": message}

    return json.dumps(d)


pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("mybatch", Batch(batch_size=5))  # User simply provides the batch size
    .apply("myunbatch", FlatMap(function=unbatch))
    .apply("mymap", Map(function=build_message_str))
    .sink(
        "kafkasink",
        stream_name="transformed-events",
    )  # flush the batches to the Sink
)
