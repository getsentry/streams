# 0013 — Existing Python Arroyo strategies are embedded through a pull-based delegate, not reimplemented

## Decision

Where the thing that must run in Python is not a function but a *step* — with its own
buffering, timing and output cadence — it runs behind `RuntimeOperator::PythonAdapter`, a
Rust strategy that delegates to a Python object. Two such steps exist today, the
multiprocessing step and the windowed reduce; both are pre-existing Python Arroyo
strategies wrapped unmodified rather than rewritten in Rust.

The Python side does not implement an Arroyo strategy. It implements a deliberately simpler
interface, `RustOperatorDelegate`: `submit()` accepts and stores work, `poll()` processes
and **returns** results, `flush()` drains. Rust forwards whatever `poll` returned to the next
strategy, so the delegate never holds a reference to anything downstream. Delegates are
created by a `RustOperatorFactory` rather than passed in directly.

## Alternatives

- Reimplement multiprocessing and windowed reduce natively in Rust.
- Give the Python side a real Arroyo strategy interface with a handle to the next Rust
  strategy.

## Rationale

This is meant to be a temporary solution to support more primitives quicker.
Ideally this will not survive when more native rust strategies are implemented.

## Consequences

- Returning instead of pushing makes the interface inherently asynchronous: a delegate may
  be 1:1, 1:N, N:1 or N:0, which a per-input return signature cannot express.
- Exceptions out of `submit` are the delegate's control channel — `MessageRejected` becomes
  backpressure, `InvalidMessage` routes to the DLQ, anything else panics.
- The factory is where state too expensive to rebuild lives — notably a pre-initialised
  process pool — because Arroyo tears down and rebuilds the chain on every rebalance.
- Output messages are Arroyo "any messages", so a failure downstream of this operator cannot
  be attributed to a specific offset.
- Payloads must pickle to reach a worker process, and the Rust `pyclass` types cannot, which
  is why Python code sees the `Message` wrappers that build the Rust object lazily.

## See also

[The Python operator](../rust-arroyo/python-operator.md) ·
[Rebalance and shutdown](../guarantees.md#rebalance-and-shutdown)
