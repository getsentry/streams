from datetime import timedelta

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import streaming_source
from sentry_streams.pipeline.chain import (
    Batch,
    Parser,
    Serializer,
)
from sentry_streams.pipeline.msg_parser import json_parser, json_serializer
from sentry_streams.pipeline.window import SlidingWindow

# The simplest possible pipeline.
# - reads from Kafka
# - parses the event
# - filters the event based on an attribute
# - serializes the event into json
# - produces the event on Kafka


# def parse(msg: str) -> Mapping[str, Any]:
#     try:
#         parsed = loads(msg)
#     except JSONDecodeError:
#         return {"type": "invalid"}

#     return cast(Mapping[str, Any], parsed)


# def filter_not_event(msg: Message[Mapping[str, Any]]) -> bool:
#     return bool(msg.payload["type"] == "event")


# def serialize_msg(msg: Mapping[str, Any]) -> str:
#     return dumps(msg)


# class TransformerBatch(Accumulator[Message[IngestMetric], Message[MutableSequence[IngestMetric]]]):

#     def __init__(self) -> None:
#         self.batch: MutableSequence[Any] = []

#     def add(self, value: Message[IngestMetric]) -> Self:
#         self.batch.append(value.payload)
#         self.schema = value.schema

#         return self

#     def get_value(self) -> Any:
#         return Message(self.schema, self.batch)

#     def merge(self, other: Self) -> Self:
#         self.batch.extend(other.batch)

#         return self


reduce_window = SlidingWindow(window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2))

pipeline = (
    streaming_source(
        name="myinput",
        stream_name="ingest-metrics",
    )
    .apply("parser", Parser(deserializer=json_parser, msg_type=IngestMetric))
    .apply("batcher", Batch(batch_size=5))
    .apply("serializer", Serializer(serializer=json_serializer))
    .sink(
        "kafkasink2",
        stream_name="transformed-events",
    )  # flush the batches to the Sink
)
