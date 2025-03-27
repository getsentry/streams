from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline import Filter, Map, streaming_source

# The simplest possible pipeline.
# - reads from Kafka
# - parses the event
# - filters the event based on an attribute
# - serializes the event into json
# - produces the event on Kafka


def parse(msg: str) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def filter_not_event(msg: Mapping[str, Any]) -> bool:
    return bool(msg["type"] == "event")


pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("mymap", Map(function=parse))
    .apply("myfilter", Filter(function=filter_not_event))
    .apply("serializer", Map(function=lambda msg: dumps(msg)))
    .sink(
        "kafkasink2",
        stream_name="transformed-events",
    )  # flush the batches to the Sink
)
