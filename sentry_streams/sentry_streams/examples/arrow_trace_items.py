"""Decode a batch of TraceItem protobufs into an Apache Arrow RecordBatch in Rust.

``ArrowBatchParser`` fuses batching and decoding: it reads the raw Kafka payloads
without copying them into Python memory, and hands the result over as an
``ArrowRecordBatch``. Any consumer implementing the Arrow PyCapsule interface can
read it -- here, polars.

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


def summarize(msg: Message[object]) -> bytes:
    """Read the Arrow batch through polars and emit a one-line summary.

    ``pl.DataFrame(batch)`` goes through ``__arrow_c_stream__``; nothing is
    converted row by row.
    """
    df = pl.DataFrame(msg.payload)

    by_type = df.group_by("item_type").agg(pl.len().alias("rows")).sort("rows", descending=True)
    summary = ", ".join(f"{row[0]}={row[1]}" for row in by_type.iter_rows())
    return f"{df.height} trace items ({summary})".encode()


pipeline = (
    streaming_source(name="myinput", stream_name="snuba-items")
    # Must come before any step that turns messages into Python objects: it reads
    # the raw payloads. Placing it later is a build-time error.
    .apply(ArrowBatchParser(name="parse_arrow", batch_size=1000))
    .apply(Map(name="summarize", function=summarize))
    .sink(StreamSink[bytes](name="mysink", stream_name="transformed-events"))
)
