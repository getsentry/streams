from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, FlatMap, streaming_source
from sentry_streams.pipeline.batch import unbatch
from sentry_streams.pipeline.chain import Parser, Serializer, StreamSink

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
chain1 = pipeline.apply("parser", Parser(msg_type=IngestMetric)).apply(
    "mybatch", Batch(batch_size=5)
)  # User simply provides the batch size

chain2 = chain1.apply(
    "myunbatch",
    FlatMap(function=unbatch),
)

chain3 = chain2.apply("serializer", Serializer()).sink(
    "kafkasink2", StreamSink(stream_name="transformed-events")
)  # flush the batches to the Sink
