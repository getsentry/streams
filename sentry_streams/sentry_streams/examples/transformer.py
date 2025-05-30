from datetime import timedelta
from typing import MutableSequence, Self

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import streaming_source
from sentry_streams.pipeline.chain import (
    Parser,
    Reducer,
    Serializer,
    StreamSink,
)
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.window import SlidingWindow

# The simplest possible pipeline.
# - reads from Kafka
# - parses the metric data, validating against schema
# - batches messages together, emits aggregate results based on sliding window configuration
# - serializes the result into bytes
# - produces the event on Kafka


class TransformerBatch(Accumulator[Message[IngestMetric], MutableSequence[IngestMetric]]):

    def __init__(self) -> None:
        self.batch: MutableSequence[IngestMetric] = []

    def add(self, value: Message[IngestMetric]) -> Self:
        self.batch.append(value.payload)

        return self

    def get_value(self) -> MutableSequence[IngestMetric]:
        return self.batch

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
        msg_type=IngestMetric,
    ),  # pass in the standard message parser function
)  # ExtensibleChain[Message[IngestMetric]]

chain2 = chain1.apply(
    "custom_batcher", Reducer(reduce_window, TransformerBatch)
)  # ExtensibleChain[Message[MutableSequence[IngestMetric]]]

chain3 = chain2.apply(
    "serializer",
    Serializer(),  # pass in the standard message serializer function
)  # ExtensibleChain[bytes]

chain4 = chain3.sink("kafkasink2", StreamSink(stream_name="transformed-events"))  # Chain
