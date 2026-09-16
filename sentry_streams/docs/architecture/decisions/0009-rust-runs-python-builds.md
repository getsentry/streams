# 0009 — Rust owns the runtime loop; Python builds the pipeline

## Decision

The production runtime is an Arroyo consumer written in Rust, exposed to Python as a PyO3
extension module. The process starts as Python, imports the extension, and hands control
over: **Python builds** a description of the pipeline and passes it to Rust operator by
operator; **Rust runs**, owning the main loop, polling Kafka, submitting through the
strategy chain and committing; **Rust calls back into Python** for application logic.

The governing rule for what goes where is that **any primitive that can be executed
entirely in Rust is implemented in Rust**, because crossing the boundary costs a GIL
acquisition, serialises the step against every other Python interaction in the process, and
may cost a copy.

## Alternatives

- A pure Python runtime on Python Arroyo (the pre-existing adapter, still in the tree).
- A pure Rust process with no embedded interpreter, requiring application logic in Rust.
- A Rust runtime that starts the Python interpreter and calls into Python
  for Python logic.

## Rationale

This platform is architected to support Python applications, Rust applications
and hybrid ones. Managing the main loop in Rust allows us to get the best
performance specifically from Rust applications.

This allows us to support these scenarios:

* For pure Rust applications data would never move into Python memory and the
  GIL is never used. This is not fully implemented though.

* There can be two types of Python hybrid applications, one where each message
  moves to Python memory and the other where the logic is written in Python
  but the processing happens in Rust.

The last scenario is particularly important, it can be implemented this way:

- Use a declarative query language to define the application logic in Python
  with DataFusion

- The processing, though, happens fully in Rust and is done by DataFusion.

Running the loop in Rust allows us not to move data to Python for DataFusion
processing.

Making the runner a Python process is done for expedience though. We built the
DSL and the first runtime in Python, most applications are written in Python
or are in Python code bases.

Having a Python DSL is desirable to support those. We could still have a Rust
runner to replace the Python one (which will be desirable), it is just not implemented
yet.

## Consequences

- Kafka consumption, offset management, the strategy chain, backpressure, DLQ routing,
  watermarks, route dispatch, batching, header filtering, broadcast and all sinks are Rust.
- Application maps, filters and routing functions, parsing and serialisation, windowed
  reduce and multiprocessing remain Python.
- Every design question in this directory — the message model, routes, the delegate
  interface — exists because of this boundary.
- A middle ground is available: a function written in Rust, exposed as a Python callable,
  releasing the GIL internally.

## See also

[Execution model](../rust-arroyo/README.md#execution-model) ·
[What runs where](../rust-arroyo/README.md#what-runs-where)
