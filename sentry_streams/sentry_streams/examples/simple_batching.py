from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline import Batch, streaming_source
from sentry_streams.pipeline.pipeline import Parser, Serializer, StreamSink

pipeline = streaming_source(
    name="myinput",
    stream_name="ingest-metrics",
)

# TODO: Figure out why the concrete type of InputType is not showing up in the type hint of chain1
chain1 = (
    pipeline.apply(Parser("parser", msg_type=IngestMetric))
    .apply(Batch("mybatch", batch_size=3))
    .apply(Serializer("serializer"))
    .sink(StreamSink("mysink", stream_name="transformed-events"))
)  # flush the batches to the Sink
