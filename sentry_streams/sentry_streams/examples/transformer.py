from datetime import timedelta
from typing import Any, MutableSequence, Optional, Self

from sentry_kafka_schemas.codecs import Codec
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import streaming_source
from sentry_streams.pipeline.chain import (
    Parser,
    Reducer,
    Serializer,
)
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_parser import msg_parser, msg_serializer
from sentry_streams.pipeline.window import SlidingWindow

# The simplest possible pipeline.
# - reads from Kafka
# - parses the event
# - filters the event based on an attribute
# - serializes the event into json
# - produces the event on Kafka


class TransformerBatch(Accumulator[Message[IngestMetric], Message[MutableSequence[IngestMetric]]]):

    def __init__(self) -> None:
        self.batch: MutableSequence[IngestMetric] = []
        self.schema: Optional[Codec[Any]] = None

    def add(self, value: Message[IngestMetric]) -> Self:
        self.batch.append(value.payload)
        if self.schema is None:
            self.schema = value.schema

        return self

    def get_value(self) -> Message[MutableSequence[IngestMetric]]:
        return Message(self.batch, self.schema)

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


reduce_window = SlidingWindow(window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2))

pipeline = streaming_source(
    name="myinput", stream_name="ingest-metrics"
)  # ExtensibleChain[Message[bytes]]

chain1 = pipeline.apply(
    "parser",
    Parser(
        msg_type=IngestMetric, deserializer=msg_parser
    ),  # pass in the standard message parser function
)  # ExtensibleChain[Message[IngestMetric]]

chain2 = chain1.apply(
    "custom_batcher", Reducer(reduce_window, TransformerBatch)
)  # ExtensibleChain[Message[MutableSequence[IngestMetric]]]

chain3 = chain2.apply(
    "serializer",
    Serializer(serializer=msg_serializer),  # pass in the standard message serializer function
)  # ExtensibleChain[bytes]

chain4 = chain3.sink(
    "kafkasink2",
    stream_name="transformed-events",
)  # Chain
