"""Decode a batch of TraceItem protobufs into an Apache Arrow RecordBatch in Rust.

``ArrowBatchParser`` fuses batching and decoding: it reads the raw Kafka payloads
without turning the individual messages into Python objects, and hands the batch
over as an Arrow IPC stream -- ordinary ``bytes``, read here with polars.

Compare with ``parquet_serializer.py``, which does the same job as
``Batch`` -> ``Map(extract_bytes)`` -> ``BatchParser`` and pays a Python round
trip per message.

Run with::

    python -m sentry_streams.runner \
        --name arrow-trace-items \
        --config deployment_config/arrow_trace_items.yaml \
        sentry_streams/examples/arrow_trace_items.py
"""

import polars as pl

from sentry_streams.pipeline import ArrowBatchParser, StreamSink, streaming_source
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import Map

TOPIC = "snuba-items"


def summarize(msg: Message[bytes]) -> bytes:
    """Read the Arrow batch through polars and emit a one-line summary.

    The payload is an Arrow IPC stream; polars reads all of it at once, with no
    row-by-row conversion.
    """
    df = pl.read_ipc_stream(msg.payload)

    by_type = df.group_by("item_type").agg(pl.len().alias("rows")).sort("rows", descending=True)
    summary = ", ".join(f"{row[0]}={row[1]}" for row in by_type.iter_rows())
    return f"{df.height} trace items ({summary})".encode()


pipeline = (
    streaming_source(name="myinput", stream_name=TOPIC)
    # Takes bytes, so it must come before any step that turns messages into
    # Python objects. Placing it later is a type error.
    .apply(ArrowBatchParser(name="parse_arrow", schema_name=TOPIC, batch_size=1000))
    .apply(Map(name="summarize", function=summarize))
    .sink(StreamSink[bytes](name="mysink", stream_name="transformed-events"))
)
