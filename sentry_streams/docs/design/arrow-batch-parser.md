# Arrow Batch Parser — PoC Implementation Plan

**Status:** implemented, phases 0-6 (see git history on `fpacifici/arrow_batches`)
**Scope:** proof of concept — protobuf only, `sentry_protos.snuba.v1.TraceItem` only

## Goal

A pipeline primitive that batches raw Kafka payloads and decodes them into an Apache
Arrow `RecordBatch` entirely in Rust, with no per-message round trip through Python.
The result is handed to Python as a `RoutedValuePayload::PyStreamingMessage`.

It replaces the Python path `Batch` → `Map(extract_bytes)` → `BatchParser`, in which
every message is copied into Python memory as `bytes` and decoded by
`sentry_kafka_schemas` under the GIL.

### PoC scope

Protobuf only, hand-written extractor per message type, Arrow schema hardcoded in the
extractor. Only `TraceItem` implemented. The Python DSL declares no schema.

### Non-goals

JSON and msgpack topics (a JSON topic is a startup panic); descriptor-driven generic
extraction; per-row dead-lettering; `TraceItem.outcomes`; replacing the Python
`BatchParser`, which keeps working on all adapters.

## Decisions

| # | Decision |
|---|---|
| 1 | **Fused step** — batching and decoding in one primitive, not a parser after `Batch`. |
| 2 | Output is a `#[pyclass]` implementing the **Arrow PyCapsule interface**. No `pyarrow` dependency. |
| 4 | New primitive; Python `BatchParser` untouched. |
| 5 | Decoding runs **inline** on the consumer thread. |
| 9 | Offsets collapse to `max` per partition, as `batch_step.rs` does today. |
| 10/17 | Input contract **`RawMessage` only** — build-time check in the adapter, runtime backstop. |
| 11 | **Generalize `BatchStep`** over a flush-producer trait rather than forking it. |
| 12 | `Reduce` subclass, `StepType.REDUCE`, `isinstance` branch in `reduce()`. |
| 14 | Failure is `panic!`, matching `transformer.rs:45-46`. |
| 22 | `map<string, AnyValue>` → **type-split maps** `attr_str/int/double/bool/bytes`. |
| 23 | **Protobuf only.** |
| 24 | `sentry-protos` for types and `prost` decode; `sentry-kafka-schemas` (`default-features = false`) for topic → schema. |
| 25 | Extractors indexed by the **raw resource string**; several topics sharing a schema share one extractor. |
| 26 | Resolution and validation at **step construction**; failures panic at startup. |
| 27 | `TraceItem` schema: all fields except `outcomes`. |

**Why no descriptor pool.** An earlier iteration used `prost-reflect` + `DescriptorPool`
for `get_field_by_name` — Rust's only equivalent of what Python's `ProtobufCodec` gets
free from protobuf's reflective runtime. Dropped for the PoC. The trade is explicit:
**adding a column requires a Rust change and a release here.** See *Deferred*.

**Why `default-features = false`.** `validate_protobuf` is the only item behind
`type_generation`, and we decode with `TraceItem::decode` directly. `get_schema`,
`schema_type` and `raw_schema` sit outside it. Disabling drops `typify`, `syn`,
`prettyplease`, `schemars` and the crate's own `prost`/`sentry_protos` pins.

## Integration points in existing code

Constraints discovered by reading the runtime. These drive several choices below.

| Fact | Location | Consequence |
|---|---|---|
| `operators::build()` receives no topic or schema. `build_chain` has `schema` but does not pass it down. | `src/operators.rs:125`, `src/consumer.rs` | The schema name must be **carried in the `RuntimeOperator` variant**, supplied by the Python adapter. Do not widen `build()`. |
| The adapter captures `schema_name = step.stream_name` *before* `override_config` can change the topic. | `rust_arroyo.py:303` | Schema lookup survives deployment topic overrides. The adapter must stash it per source so `reduce()` can read it. |
| Source wraps every payload in a `Py<RawMessage>` immediately. | `consumer.rs::to_routed_value` | Payload bytes live in **Python-owned memory**; reading them needs the GIL. **Temporary** — see *Assumed future work*. |
| `Batch` stores `committable` (`offset+1`), collapsed to `max` per partition. | `batch_step.rs:104` | No per-row offsets → no DLQ. Consequence 1. |
| A non-`InvalidMessageError` Python exception, or any error on an `AnyMessage`, panics. | `transformer.rs:45-46` | Panicking is consistent with the runtime's existing behaviour, not a new failure mode. |
| `StrategyError::InvalidMessage` makes arroyo **continue**; `StrategyError::Other` stops the consumer. | `processing/mod.rs:321-349` | Neither is usable for a batch with collapsed offsets; hence `panic!`. |

## `TraceItem` Arrow schema

| Column | Arrow type | Null | Source |
|---|---|---|---|
| `organization_id` | `UInt64` | no | 1 |
| `project_id` | `UInt64` | no | 2 |
| `trace_id` | `Utf8` | no | 3 |
| `item_id` | `Binary` | no | 4 (bytes, little endian) |
| `item_type` | `Utf8` | no | 5, enum **name** via `as_str_name()` |
| `timestamp` | `Timestamp(us, "UTC")` | **yes** | 6 |
| `client_sample_rate` | `Float64` | no | 8 |
| `server_sample_rate` | `Float64` | no | 9 |
| `conversation_id` | `Utf8` | no | 10 |
| `session_id` | `Utf8` | no | 11 |
| `retention_days` | `UInt32` | no | 100 |
| `received` | `Timestamp(us, "UTC")` | **yes** | 101 |
| `downsampled_retention_days` | `UInt32` | no | 102 |
| `attr_str` | `Map<Utf8, Utf8>` | no | 7, `AnyValue` arm 1 (+ 5, 6 JSON-encoded) |
| `attr_int` | `Map<Utf8, Int64>` | no | 7, arm 3 |
| `attr_double` | `Map<Utf8, Float64>` | no | 7, arm 4 |
| `attr_bool` | `Map<Utf8, Boolean>` | no | 7, arm 2 |
| `attr_bytes` | `Map<Utf8, Binary>` | no | 7, arm 7 |

**Nullability follows protobuf presence, not the `optional` keyword.** Message-typed
fields always have explicit presence in proto3, so prost yields `Option<Timestamp>`
for *both* `timestamp` and `received` — both nullable. Implicit-presence scalars cannot
distinguish unset from zero, so they are non-nullable columns carrying the default.

> **Corrected during phase 3.** This table originally marked `conversation_id` and
> `session_id` nullable, on the assumption they were declared `optional`. In
> `sentry_protos` 0.70.0 both are plain `String`, not `Option<String>` — implicit
> presence, despite the "if any" comments in the proto. Applying the rule above,
> they are non-nullable columns carrying `""` when unset. Should they ever gain
> `optional` upstream, prost will change their type and the extractor will fail to
> compile, which is the right way to find out.

`ArrayValue` (arm 5) and `KeyValueList` (arm 6) are recursive; Arrow has no recursive
types, so they are JSON-encoded into `attr_str`. Bytes *nested inside* such a value
are base64-encoded, following proto3's canonical JSON mapping — there is no way to put
raw bytes in a JSON string. This is not the case the plan rejected earlier: top-level
`bytes` attributes never pass through JSON, they keep their raw bytes in `attr_bytes`.

## Dependencies

```toml
arrow = { version = "59", features = ["ffi"] }
prost = "0.14"
prost-types = "0.14"          # prost_types::Timestamp, reached through TraceItem
base64 = "0.22"               # bytes nested in recursive attribute values
sentry_protos = "0.70"
sentry-kafka-schemas = { version = "3", default-features = false }
```

`prost-types` and `base64` were added in phase 3; both were already in the lock file
transitively, so neither costs build time.

No new Python dependencies; `pyarrow` is deliberately not added.

---

# Phases

Phases 1 and 2 are independent and may run in parallel. 3 depends on 0; 4 on 1+2+3;
5 on 4; 6 on 5.

## Phase 0 — Dependencies

1. Add the four crates above.
2. **Verify `default-features = false` still exposes** `get_schema`, `Schema::schema_type`
   and `Schema::raw_schema`. If `raw_schema` turns out to be gated, fall back to default
   features and record the build-time cost in this document.
3. `cargo build`, `cargo test` green with no code changes.

**Acceptance:** clean build; a throwaway test asserting
`get_schema("snuba-items", None).unwrap().raw_schema()` equals
`"sentry_protos.snuba.v1.trace_item_pb2.TraceItem"`.

## Phase 1 — `PyRecordBatch` (Arrow → Python)

**File:** `src/py_record_batch.rs` *(new)*; register in `src/lib.rs`; stubs in
`sentry_streams/rust_streams.pyi`.

```rust
#[pyclass(name = "ArrowRecordBatch", module = "sentry_streams.rust_streams")]
pub struct PyRecordBatch { pub(crate) batch: RecordBatch }

#[pymethods]
impl PyRecordBatch {
    #[getter] fn num_rows(&self) -> usize;
    #[getter] fn num_columns(&self) -> usize;
    fn __repr__(&self) -> String;

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(&self, py: Python<'py>, requested_schema: Option<Bound<'py, PyAny>>)
        -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)>;

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>>;

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(&self, py: Python<'py>, requested_schema: Option<Bound<'py, PyAny>>)
        -> PyResult<Bound<'py, PyCapsule>>;
}
```

**Implement all three, not just `__arrow_c_array__`.** Table-level consumers —
`pl.DataFrame(obj)`, `pa.table(obj)` — look for `__arrow_c_stream__`; array-level
consumers use `__arrow_c_array__`. Implementing only one makes the object work in some
call sites and not others.

Mechanics:

- Array: `StructArray::from(batch.clone())` → `arrow::ffi::to_ffi(&struct_array.to_data())`
  → two capsules.
- Stream: `FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(...)))`, one batch.
- **Capsule names must be exactly** `arrow_schema`, `arrow_array`, `arrow_array_stream`,
  as NUL-terminated `CString`. A wrong name fails at the consumer with an opaque error.
- `PyCapsule::new` takes ownership; `FFI_ArrowSchema`/`FFI_ArrowArray`'s `Drop` invokes
  the C release callback, so no manual destructor is needed.
- `requested_schema` is accepted and **ignored** — the protocol permits returning the
  native schema when a cast is unsupported. Document it in the docstring.

**Tests**

| Test | Assertion |
|---|---|
| `polars.DataFrame(rb)` | values, column names, dtypes match |
| `pyarrow.record_batch(rb)` *(dev-dep only)* | round-trips `__arrow_c_array__` |
| `pyarrow.table(rb)` | round-trips `__arrow_c_stream__` |
| consume twice | second call still yields a valid batch (no double-release) |
| nested `Map` column | survives the FFI boundary |

**Acceptance:** a Rust-built `RecordBatch` reaches polars with correct values and
schema. pyarrow may be a dev-only dependency; it must not enter runtime deps.

## Phase 2 — Generalize `BatchStep` (pure refactor)

**File:** `src/batch_step.rs`.

```rust
pub(crate) trait BatchFlushProducer: Send + Sync {
    fn produce(
        &self,
        route: &Route,
        elements: &[PyStreamingMessage],
        committable: BTreeMap<Partition, u64>,
    ) -> Result<Message<RoutedValue>, StrategyError>;
}

/// Existing behaviour, lifted verbatim out of `Batch::flush`.
pub(crate) struct PyListFlushProducer;
```

1. Move the body of `Batch::flush` (`batch_step.rs:166-199`) into
   `PyListFlushProducer::produce`. Do not change it.
2. `Batch` gains `producer: Arc<dyn BatchFlushProducer>`; `flush()` delegates.
3. `BatchStep::new` takes the producer; `build_batch_step` keeps its signature and
   passes `PyListFlushProducer`.
4. Leave untouched: watermark buffering, `pending_batch`, `drain_outbound`,
   `record_rejected_submit`, `oldest_batch_row_timestamp`, `join`.

**Acceptance:** **no behaviour change.** The existing `batch_step.rs` test module
passes unmodified — not adapted, unmodified. Lands as its own commit so any regression
is attributable.

## Phase 3 — Extractor module

**Files:** `src/extractors/mod.rs`, `src/extractors/trace_item.rs` *(new)*.

```rust
#[derive(Debug)]
pub enum ExtractorError {
    Decode { index: usize, source: prost::DecodeError },
    Build(arrow::error::ArrowError),
    Field { field: &'static str, detail: String },
}

pub trait Extractor: Send + Sync {
    fn resource(&self) -> &'static str;
    fn schema(&self) -> SchemaRef;
    fn extract(&self, payloads: &[&[u8]]) -> Result<RecordBatch, ExtractorError>;
}

pub fn get_extractor(resource: &str) -> Option<&'static dyn Extractor>;
pub fn registered_resources() -> Vec<&'static str>;
```

`extract` is **batch-wise**: one Arrow builder per column, fed across all rows, finished
once. Not row-at-a-time — that is the shape Arrow builders want and it keeps a second
extractor down to one file plus one registry line.

### `TraceItem` extractor — implementation notes

- **`MapBuilder` field names.** arrow-rs defaults to `entries`/`keys`/`values`; the Arrow
  spec is `entries`/`key`/`value`. Set `MapFieldNames` explicitly to the spec names or
  pyarrow/polars interop breaks in confusing ways.
- **`append(true)` per row on every map builder**, including rows with no attributes of
  that type. Skipping it silently misaligns every subsequent row.
- **Sort attribute keys per row.** prost decodes `map<string, AnyValue>` into a
  `HashMap` with nondeterministic iteration order; unsorted output makes batches
  irreproducible and tests flaky.
- **Unknown enum values must not panic.** `TraceItemType::try_from(i32)` fails on a value
  from a newer producer; render `TRACE_ITEM_TYPE_UNKNOWN_<n>` (the enum's own
  prefix, so string comparisons downstream stay uniform). Enum additions are routine
  forward-compatible producer changes and crashing on them would be a self-inflicted
  outage. *(Flagged — override if you want strictness.)*
- **Timestamps:** `prost_types::Timestamp { seconds, nanos }` →
  `seconds * 1_000_000 + nanos / 1_000`, with `checked_mul`/`checked_add` and an
  `ExtractorError::Field` on overflow rather than a silent wrap.
- Builders: `UInt64Builder`, `StringBuilder`, `BinaryBuilder`,
  `TimestampMicrosecondBuilder::new().with_timezone("UTC")`, `Float64Builder`,
  `UInt32Builder`, and five `MapBuilder<StringBuilder, _>`.

**Tests** (`src/extractors/trace_item.rs`, `mod tests`) — build `TraceItem` values
in-process with prost, encode, extract, assert:

| Case | Expectation |
|---|---|
| all scalar fields populated | every column matches |
| each `AnyValue` arm (string, bool, int, double, bytes) | lands in its own `attr_*` map |
| `ArrayValue` / `KeyValueList` | JSON-encoded into `attr_str` |
| absent `timestamp` / `received` | null, not epoch zero |
| absent `conversation_id` / `session_id` | null |
| unset implicit-presence scalars | zero/empty, non-null |
| unknown `item_type` number | `TYPE_UNKNOWN_<n>`, no panic |
| same attributes, different insertion order | byte-identical `RecordBatch` |
| rows with 0, 1, many attributes mixed in one batch | map offsets aligned |
| truncated payload | `ExtractorError::Decode { index }` naming the row |
| `Timestamp` at i64 boundary | `ExtractorError::Field`, no wrap |

**Acceptance:** the table above passes; `extract` on an empty slice yields a
zero-row batch with the correct schema.

## Phase 4 — The parser step

**File:** `src/arrow_batch_parser.rs` *(new)*.

```rust
pub(crate) struct ArrowFlushProducer {
    extractor: &'static dyn Extractor,
    step_name: String,
}
impl BatchFlushProducer for ArrowFlushProducer { /* ... */ }

pub fn build_arrow_batch_parser_step(
    route: &Route,
    schema_name: &str,
    step_name: String,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>>;
```

Construction-time resolution, each failure a `panic!` naming step, topic and cause:

1. `get_schema(schema_name, None)` — panic if the topic is unknown.
2. `schema.schema_type == SchemaType::Protobuf` — panic otherwise, naming the actual type.
3. `schema.raw_schema()` → resource string.
4. `get_extractor(resource)` — panic if absent, listing `registered_resources()`.

`get_schema` runs **exactly once here.** It `Box::leak`s on protobuf topics and must
never be called per message.

`produce` then:

1. Calls `with_payloads` (below) to obtain `&[&[u8]]`, rejecting any `PyAnyMessage`
   element with a panic naming the step (the decision-10 runtime backstop).
2. Calls `extractor.extract` inside that scope.
3. Wraps the `RecordBatch` in `PyRecordBatch`, then a `PyAnyMessage` with
   `schema = Some(schema_name)` and the flush timestamp, exactly as
   `PyListFlushProducer` does.
5. Emits `RoutedValuePayload::PyStreamingMessage`.

### The payload seam — get this shape right the first time

`consumer.rs::to_routed_value` boxes every payload into a `Py<RawMessage>` at the
source, so payload bytes currently live in Python-owned memory and reading them needs
the GIL. Extraction therefore holds the GIL for the duration of a batch decode. That is
**temporary** (see *Assumed future work*), and the code must be written so removing it
is a deletion rather than a refactor.

```rust
/// The only place that knows where payload bytes live.
fn with_payloads<R>(elements: &[BatchElement], f: impl FnOnce(&[&[u8]]) -> R) -> R;
```

Today: acquire the GIL, collect `PyRef<RawMessage>` guards, borrow `&[u8]` from each,
call `f`. Once the source emits native messages: collect the slices and call `f`. The
call site does not change. It is a scope rather than a plain function because the
`PyRef` guards must outlive the slices.

**Do not copy payloads out of Python memory to avoid holding the GIL.** Collecting
`Vec<Vec<u8>>` and releasing the GIL would work today and would become permanent dead
weight the moment the source goes native — a per-message copy in the one step whose
purpose is to eliminate per-message copies.

`Extractor::extract` takes `&[&[u8]]` precisely so that it, and every test in phase 3,
is indifferent to where the bytes live. The **output** side is unaffected either way:
the result must remain a `PyStreamingMessage`, since Python consumes the `RecordBatch`.

**Tests:** flush producing a correct batch; mixed `PyAnyMessage` in the window panics;
an empty window produces nothing; committable and the synthetic watermark match
`PyListFlushProducer`'s behaviour for the same input.

## Phase 5 — DSL and adapter wiring

**`src/operators.rs`** — new variant. Note `schema_name`, per *Integration points*:

```rust
#[pyo3(name = "ArrowBatchParser")]
ArrowBatchParser {
    route: Route,
    step_name: String,
    schema_name: String,
    max_batch_size: Option<usize>,
    max_batch_time_ms: Option<f64>,
},
```

with a `build()` arm mirroring `RuntimeOperator::Batch` (`operators.rs:232-240`),
converting ms → `Duration` the same way.

**`sentry_streams/pipeline/pipeline.py`** — `ArrowBatchParser`, a `Reduce` subclass
carrying `batch_size` and `batch_timedelta` only. `override_config` and `validate`
mirror `Batch` (`pipeline.py:674-681`). No schema, no format, no type name.

**`adapters/arroyo/rust_arroyo.py`**

1. In `source()`, stash the pre-override schema name:
   `self.__source_schemas[source_name] = schema_name`.
2. In `reduce()`, add an `isinstance(step, ArrowBatchParser)` branch before the `Batch`
   branch, emitting `RuntimeOperator.ArrowBatchParser(..., schema_name=self.__source_schemas[stream.source], ...)`.
3. Build-time input check. *(Implemented differently from the sketch: `reduce()` is
   handed only the step and the `Route`, never the pipeline graph, so the walk is not
   available.)* The adapter instead tracks which routes still carry raw payloads — the
   source marks its route raw, `map`/`flat_map` and every other `reduce` clear it, and
   filters, `broadcast` and `router` propagate it, since they forward messages
   untouched. `ArrowBatchParser` raises if its route is not raw.

**`adapters/arroyo/adapter.py`** — `NotImplementedError` pointing at the Rust adapter,
documented Rust-only in the style of `HeadersFilter`.

**Also:** export from `pipeline/__init__.py` (both the import and `__all__`); add
`RuntimeOperator.ArrowBatchParser` and `ArrowRecordBatch` to `rust_streams.pyi`.

**Tests:** placing the step after a Python `Map` fails at build time naming the step;
a filter between source and parser is accepted; a deployment topic override does not
change the resolved schema;
a JSON topic panics at startup with the actual `schema_type`; the pure-Python adapter
raises `NotImplementedError`; `make typecheck` clean.

## Phase 6 — Example, end-to-end, benchmark

1. `sentry_streams/examples/arrow_trace_items.py` — `snuba-items` → `ArrowBatchParser`
   → a `Map` consuming the batch via polars.
2. End-to-end test through the full step with real `TraceItem` payloads.
3. **Benchmark against `Batch` + `BatchParser`.** Implemented as an `#[ignore]`d test
   rather than a criterion suite, so it adds no dependency:
   `cargo test --release bench_arrow_vs_pylist -- --ignored --nocapture`.

   **Results** (10 000 rows/window, `TraceItem` with one string attribute, 20 windows):

   | Path | p50 | p99 | rows/s (p50) |
   |---|---|---|---|
   | `ArrowBatchParser` | 3.5 ms | 4.1 ms | 2.86 M |
   | `Batch` → `BatchParser` | 4.3 ms | 6.4 ms | 2.33 M |
   | `Batch` flush alone (not a complete path) | 0.15 ms | 0.20 ms | 65.8 M |

   About **20% faster at p50 and 35% at p99** — real, but well short of what the "no
   Python round trip" framing suggests, and worth being straight about. Two caveats
   both point the same way: the comparison stops at *decoded values*, where the Python
   path still has to build something columnar from those objects, and the Arrow path is
   still paying the `Py<RawMessage>` copy and holding the GIL (see *Assumed future
   work*). Re-run once the source goes native.

   Decision 5 holds comfortably: a 1000-row window decodes in well under a millisecond,
   nowhere near `max_poll_interval_ms`. Threadpool decoding stays deferred.
4. Docs page: the hardcoded-schema contract, Rust-adapter-only, failure behaviour.

---

## Accepted consequences

1. **A malformed message panics the process.** Decisions 9 and 14: one bad payload fails
   the whole batch, cannot be dead-lettered, and crash-loops on restart since the
   consumer re-reads the same offsets. Arroyo's DLQ needs an exact `(partition, offset)`;
   batching collapses offsets to `max`.
2. **The Arrow schema lives in Rust.** Adding a column is a code change and a release —
   the explicit PoC trade for dropping the descriptor pool.
3. **An attribute changing type between messages lands in different columns** across
   batches. Inherent to decision 22; Snuba EAP has the same property.
4. **Recursive attribute values become JSON strings**, not structured data.
5. **Protobuf only.** A JSON or msgpack topic panics at startup.
6. **Rust adapter only**, unlike the Python `BatchParser`.
7. **Decoding holds the GIL** for the batch and stalls the consumer loop. Temporary,
   and confined to `with_payloads`; see phase 4 and *Assumed future work*.

## Assumed future work (owned elsewhere)

**The source will stop boxing payloads into Python memory.** `RoutedValuePayload` will
carry a Rust-native message — `messages.rs` already has an unused `StreamingMessage`
enum reserved for it — instead of `Py<RawMessage>` built by `into_pyraw` per message.

This is outside the scope of this work, but the plan assumes it lands. When it does:

- `with_payloads` loses its `traced_with_gil!` block and its `PyRef` guards. Nothing
  else in this step changes.
- Consequence 7 disappears; decoding becomes genuinely GIL-free.
- Phase 6's benchmark should be re-run — the numbers taken before the change understate
  what the step is capable of.
- The `BatchElement` type alias used by `BatchFlushProducer` and `with_payloads` is
  where the element type changes; it exists so that swap is one line.

Decision 1 (fused rather than a parser after `Batch`) is unaffected: `Batch`'s flush
still builds a Python list of `bytes`, so a separate parser would still pay a round trip
even once the source is native.

## Deferred

- **Descriptor-driven extraction** — `prost-reflect` + `DescriptorPool` restores
  `get_field_by_name`, making a new column configuration rather than a release.
  Descriptors from vendored `.proto` compiled by `protox`, or better from an upstream PR
  adding `.file_descriptor_set_path(...)` to `sentry-protos`' generator so the crate
  ships a `FILE_DESCRIPTOR_SET` and no vendoring is needed.
- The JSON path, and whether its schema is derived from the topic's JSON Schema (needing
  a `$ref` resolver, the crate's being private) or declared.
- Per-row offset tracking, turning consequence 1 into a dead-lettered message and a
  surviving batch.
- Threadpool decoding, if the benchmark justifies it.
- `TraceItem.outcomes`; msgpack topics.
