# Messages

The message model decides what a step can and cannot do, where the GIL has to be taken,
which conversions are possible, and what a step author has to handle. Almost every
limitation of this runtime traces back to something on this page.

It is written rules-first: if you are implementing a step, the first section is the one
you must not skip, and the skeleton that applies it is in
[Adding a step](../guides/adding-a-step.md#5-implement-the-rust-strategy).

## Rules for step authors

1. **Check the route before anything else**, and forward on mismatch. No GIL.
2. **Handle watermarks explicitly.** Forward them unless the step buffers messages, in
   which case hold them until the buffered work is out.
3. **Handle every payload variant.** Support what the step can support; panic with a clear
   message on what it cannot, rather than silently passing it through.
4. **Do not convert payloads you do not need to read.** An opaque payload is a pointer
   move; a converted one is a copy plus a GIL acquisition.
5. **Take the GIL once, in the largest reasonable unit of work.**
6. **Assume messages are immutable.** Replacing a payload produces a new message. This is
   enforceable in Rust and not in Python — a Python step can mutate the object behind a
   `PyAnyMessage` — so on the Python side it is a convention.

The rest of this page is why.

## Two classes of payload

A message flowing through the Rust consumer has metadata — headers, a timestamp, an
optional schema name — and a payload. The metadata is the same for everyone; the payload
is what differs, in two fundamentally different ways.

**Rust-native payloads** live in Rust memory and can be read without the GIL. Today the
only one is `RawMessage`, whose payload is a `Vec<u8>`: the bytes read from Kafka, or the
bytes a step produced to be written out. Rust can inspect, slice, hash or write it freely.

**Python payloads** live in Python memory. `PyAnyMessage` holds a `Py<PyAny>` — a smart
pointer to an arbitrary Python object. From the Rust side it is *opaque*: it can be moved,
cloned (with the GIL) and handed back to Python, but not inspected. This is the
representation of everything between a parser and a serializer.

The distinction states who owns the data:

> A `RawMessage` is data the runtime understands. A `PyAnyMessage` is data the
> *application* understands, which the runtime only carries between the steps that do.

Both are `pyclass`es, so both can be handed to Python code — the difference is what it
costs. Handing over a `PyAnyMessage` passes a pointer; handing over a `RawMessage` builds a
Python `bytes` object, which copies.

## The enums

```
RoutedValue
├── route: Route                      which branch this message belongs to
└── payload: RoutedValuePayload
    ├── PyStreamingMessage            a data message
    │   ├── PyAnyMessage              payload is a Python object
    │   └── RawMessage                payload is a byte array
    └── WatermarkMessage              a control message
        ├── Watermark                 native, Rust memory
        └── PyWatermark               crossed into Python memory
```

Every strategy in the chain is a `ProcessingStrategy<RoutedValue>`, so every strategy
receives every variant, and the compiler makes matching on them unavoidable. That is what
rules 1–3 are about.

The "fail loudly" half of rule 3 is deliberate. The Kafka sink panics if it is given a
`PyAnyMessage`, because it has no way to turn an arbitrary Python object into bytes — that
is the serializer's job, and its absence from the pipeline is a bug in the application,
not a runtime condition to recover from. The general policy is in
[Guarantees](../guarantees.md#failure-policy).

## Conversion is limited, on purpose

The only conversion the runtime performs is **between byte arrays**: a `RawMessage` can be
exposed to Python as `bytes`, and bytes coming back from Python become a `RawMessage`
again. When a Python transform returns `bytes` the result is wrapped as a `RawMessage`;
when it returns anything else it is wrapped as a `PyAnyMessage`.

There is no conversion from `PyAnyMessage` to `RawMessage`, because there is no general way
to serialise an arbitrary Python object.

Two rules applications actually run into follow from that:

- A pipeline that writes to Kafka must serialise before the sink. Without a serializer the
  sink receives an opaque object and panics.
- A Rust-native step that needs to read the payload can only be applied where the payload
  is a `RawMessage` — that is, before any step that produced Python objects.

## Conversion costs the GIL

Anything that touches a Python payload needs the GIL, including operations that look
innocuous. Cloning a `RawMessage` is a memcpy; cloning a `PyAnyMessage` is a `clone_ref`,
which increments a Python reference count and therefore requires attaching to the
interpreter — so a broadcast fanning one message out to four branches takes the GIL to do
it. Reading the timestamp is the same: the field lives inside a `pyclass` instance.

Hence rules 4 and 5, and one further property: **GIL acquisition is instrumented.** All
Rust code acquires the GIL through the `traced_with_gil!` wrapper (`src/utils.rs`), which
logs a warning when acquisition takes longer than a threshold, because a slow acquisition
means another thread is holding it and the pipeline is serialising on Python.

## Routes

Arroyo pipelines are linear: a strategy has exactly one next strategy, and every message
visits every step in order. The DSL, however, has `Router` and `Broadcast`. Routes are how
the second is implemented on top of the first.

Every message carries a `Route`: the name of the source it came from and an ordered list of
`waypoints`, the branches it has been sent down. Every strategy is built with the route it
belongs to, and forwards untouched anything whose route differs. The chain is therefore
still physically linear and contains the steps of every branch one after another; each
message "executes" only the steps whose route it matches.

- A `Router` appends the one waypoint its routing function selected.
- A `Broadcast` emits one copy per downstream branch, each with a different waypoint.

The cost is that a message physically traverses every strategy in the consumer, including
branches it will never enter. The route check is cheap and GIL-free, which is what makes
that acceptable.

## Watermarks

Branching breaks committing, and watermarks are the fix.

Arroyo's normal commit policy commits the offsets of a message once it reaches the end of
the chain. With a broadcast there are several copies of that message in flight, and the
message is only really processed when *all* of them have finished; committing when the
first arrives would commit work that has not happened and lose it on a restart.

A **watermark** is a control message injected by the `WatermarkEmitter` at the head of the
chain. The emitter records the offsets of every message passing through it and every few
seconds (10 by default) emits a watermark carrying the accumulated committable, then
clears it.

Watermarks travel down the pipeline like data messages and go through the *same* branching
logic: a broadcast duplicates a watermark exactly as it duplicates a message, and a router
forwards one to all of its branches, because it cannot know which branches received
messages since the last one.

At the end of the chain, the commit policy does not commit on messages at all. It counts
watermarks: for a given watermark it accumulates copies, and only when it has seen one from
every branch does it turn the carried offsets into a commit request.

```
  Source
    |
┎-Router-┓
|        |
1   ┎Broadcast┓
    |    |    |
    2    3    4
```

*Four branches, so four copies of each watermark must arrive before its offsets are
committed.*

Because a watermark is committed only once every copy has arrived, its offsets correspond
to work finished on every path. Because watermarks are periodic rather than per-message,
the bookkeeping is bounded: the commit step tracks a handful of in-flight watermarks, not a
set of in-flight offsets. Trackers that never receive all their copies — a branch that
broke and stopped forwarding — are dropped after a timeout so the buffer cannot grow
without bound.

Most steps forward watermarks. The ones that do not are the steps that hold messages back:

| Step | Watermark handling |
| --- | --- |
| **Reduce** | Accumulates the committables of watermarks received while a window is open; releases the combined result when the window closes |
| **Python delegate** | Holds a watermark until the output produced covers the offsets it carries |
| **Broadcast / Router** | Duplicate them, one per branch |

Watermarks also carry the timestamp of the newest data message seen since the previous
watermark, which is how end-to-end consumer latency is measured at commit time.

### Crossing the language boundary

A watermark is a `Watermark` struct in Rust. When it must be handed to Python — a delegate
needs to see it to keep the ordering guarantee above — it is converted into `PyWatermark`, a
`pyclass` whose committable is a Python dict, and converted back on the way out. That is
why `WatermarkMessage` has two variants, and it comes with directional invariants recorded
in [Contracts](../contracts.md#the-message-model).
