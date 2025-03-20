from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
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


pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="events",
)

parser = Map(name="parser", ctx=pipeline, inputs=[source], function=parse)

filter = Filter(
    name="myfilter", ctx=pipeline, inputs=[parser], function=lambda msg: msg["type"] == "event"
)

jsonify = Map(name="serializer", ctx=pipeline, inputs=[filter], function=lambda msg: dumps(msg))

sink = KafkaSink(
    name="kafkasink",
    ctx=pipeline,
    inputs=[jsonify],
    logical_topic="transformed-events",
)
