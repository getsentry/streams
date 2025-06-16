from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, FlatMap, streaming_source
from sentry_streams.pipeline.batch import unbatch
from sentry_streams.pipeline.chain import Parser, Serializer, StreamSink

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
parsed = pipeline.apply("parser", Parser(msg_type=IngestMetric))

batched = parsed.apply("mybatch", Batch(batch_size=5))  # User simply provides the batch size

unbatched = batched.apply(
    "myunbatch",
    FlatMap(function=unbatch),
)

sinked = unbatched.apply("serializer", Serializer()).sink(
    "kafkasink2", StreamSink(stream_name="transformed-events")
)  # flush the batches to the Sink
