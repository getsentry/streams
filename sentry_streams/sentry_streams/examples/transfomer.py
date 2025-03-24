from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Pipeline,
    StreamSink,
    StreamSource,
)

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


def jsonify_msg(msg: Mapping[str, Any]) -> str:
    return dumps(msg)


pipeline = Pipeline()

source = StreamSource(
    name="myinput",
    ctx=pipeline,
    stream_name="events",
)

parser = Map(name="parser", ctx=pipeline, inputs=[source], function=parse)

filter = Filter(
    name="myfilter", ctx=pipeline, inputs=[parser], function=lambda msg: msg["type"] == "event"
)

jsonify = Map(name="serializer", ctx=pipeline, inputs=[filter], function=jsonify_msg)

sink = StreamSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[jsonify],
    stream_name="transformed-events",
)
