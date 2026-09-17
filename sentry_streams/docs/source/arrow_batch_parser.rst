Arrow Batch Parser
==================

``ArrowBatchParser`` batches raw Kafka payloads and decodes them into an Apache
Arrow ``RecordBatch`` entirely in Rust. The batch is handed to Python as an
`Arrow IPC stream <https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc>`_
-- an ordinary ``bytes`` payload, which every existing step already understands.

.. code-block:: python

   import polars as pl

   from sentry_streams.pipeline import ArrowBatchParser, StreamSink, streaming_source
   from sentry_streams.pipeline.pipeline import Map


   def summarize(msg):
       df = pl.read_ipc_stream(msg.payload)
       return f"{df.height} rows".encode()


   pipeline = (
       streaming_source(name="myinput", stream_name="snuba-items")
       .apply(
           ArrowBatchParser(
               name="parse_arrow", schema_name="snuba-items", batch_size=1000
           )
       )
       .apply(Map(name="summarize", function=summarize))
       .sink(StreamSink[bytes](name="mysink", stream_name="transformed-events"))
   )

It replaces ``Batch`` → ``Map(extract_bytes)`` → ``BatchParser``, in which every
message is copied into Python memory as ``bytes`` and decoded under the GIL.
Here the individual messages are never turned into Python objects; only the
assembled batch crosses over.

.. note::

   Serializing the batch and copying it into Python memory is a deliberate
   simplification. Handing the ``RecordBatch`` across directly, through the Arrow
   C data interface, removes both copies and is deferred rather than ruled out.

Windowing is configured exactly like :class:`Batch`, by ``batch_size`` and/or
``batch_timedelta``, both overridable from ``steps_config``.

``schema_name`` is the logical stream name whose ``sentry-kafka-schemas`` entry
names the message type, and so selects the extractor -- normally the source's
``stream_name``. It is declared on the step rather than inferred from the source,
so the step does not depend on where it sits and a deployment topic override
cannot change which extractor is used.

The schema is hardcoded
-----------------------

There is nothing to configure. Each supported message type has a hand-written
extractor in Rust that owns its Arrow schema, resolved from the source topic's
entry in ``sentry-kafka-schemas``. Several topics sharing a schema share one
extractor.

**Adding a column, or a new message type, is a Rust change and a release.** This
is the deliberate trade of the current implementation: Rust has no equivalent of
Python's reflective ``ProtobufCodec``, and carrying a protobuf descriptor pool to
get one was judged not worth it yet. See
``docs/design/arrow-batch-parser.md``.

Currently implemented: ``sentry_protos.snuba.v1.TraceItem`` (topic
``snuba-items``).

How it can fail
---------------

Every failure is loud, and most happen at startup rather than in production:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Situation
     - Result
   * - Topic has no schema, or a JSON/msgpack schema
     - Panic at startup, naming the topic and the schema type found
   * - Message type has no extractor
     - Panic at startup, listing the supported message types
   * - Step placed after a ``Map`` or other converting step
     - Type error under mypy; **panic** on the first batch if types were bypassed
   * - Used with the pure-Python Arroyo adapter
     - ``NotImplementedError``
   * - A payload fails to decode
     - **Panic, failing the process**

That last one deserves emphasis. Batching collapses offsets to the maximum per
partition, so there is no ``(partition, offset)`` for Arroyo's DLQ to reject a
single row with: one bad payload fails the whole batch, and the consumer will
re-read the same offsets on restart. This matches what the runtime already does
for an error on an ``AnyMessage``, but it means a malformed message is an outage,
not a dead letter. Per-row offset tracking is deferred work.

Placement
---------

The step reads bytes off the wire, so it must come before any step that turns
messages into Python objects. Filters, broadcasts and routers are fine — they
forward messages untouched. A ``Map`` in between is a build-time error.

Attribute columns
-----------------

``TraceItem``'s ``map<string, AnyValue>`` is split by value type into
``attr_str``, ``attr_int``, ``attr_double``, ``attr_bool`` and ``attr_bytes``.
Arrow has no usable union type here and Snuba EAP splits the same way. Two
consequences:

* an attribute that changes type between messages lands in different columns;
* recursive values (``ArrayValue``, ``KeyValueList``) are **dropped**. Arrow has
  no recursive type. ``AnyValue`` is a port of OpenTelemetry's type, so these
  arms exist because OTel has them rather than because Sentry ingestion uses
  them, and they do not appear in the canonical ``snuba-items`` example. If one
  does arrive, that attribute is skipped -- the rest of its row is unaffected --
  and nothing is logged. If they turn out to occur in practice they should get
  their own column rather than being flattened into ``attr_str``, where an
  encoded array would be indistinguishable from a string that looks like one.

Performance
-----------

Measured with ``cargo test --release bench_arrow_vs_pylist -- --ignored
--nocapture``, 10 000 rows per window, ``TraceItem`` with one string attribute:

.. list-table::
   :header-rows: 1

   * - Path
     - p50
     - p99
   * - ``ArrowBatchParser``
     - 3.6 ms
     - 4.8 ms
   * - ``Batch`` → ``BatchParser``
     - 4.2 ms
     - 6.3 ms

About 20% faster at the median and 35% at the tail — a real but modest win on
this workload, not the order of magnitude the "no Python round trip" framing
might suggest. Two things to keep in mind when reading it:

* The comparison stops at decoded values. The Python path then has to *build*
  something columnar (polars, parquet) from those objects, which this step has
  already done.
* The source still hands Rust its payloads inside Python-owned ``RawMessage``
  objects, so decoding holds the GIL and pays a copy *per message*. Removing that
  is work owned elsewhere; the numbers here understate what the step can do once
  it lands, and the benchmark should be re-run then.

At these figures a 1 000-row window decodes in well under a millisecond, so
running inline on the consumer thread is comfortably within
``max_poll_interval_ms``. Moving decoding to a threadpool is deferred until a
benchmark justifies it.
