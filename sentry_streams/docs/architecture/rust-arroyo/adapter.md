# The adapter in depth

`RustArroyoAdapter` (`sentry_streams/adapters/arroyo/rust_arroyo.py`) is the Python half of
the Rust runtime: the component the runner drives while walking the pipeline graph, which
ends up holding a fully described Rust consumer.

## The stream handle is a route

The adapter interface is generic in the type representing "a stream". This adapter uses
`Route` — the source name plus the waypoints identifying a branch.

That choice says a lot about the runtime. There is no stream object to hand around: the
consumer is a single linear chain of strategies, and the only thing distinguishing one
logical stream from another is which branch it belongs to. So when the runner asks the
adapter to attach a map to a stream, what it passes is "which branch", and what it gets
back is the branch the output belongs to — the same one for everything except router and
broadcast, which return one route per branch.

The adapter keeps a map of consumers keyed by source name. The source step creates the
consumer; every subsequent step looks up the consumer for its route's source and appends to
it. More than one consumer per process is not supported today, but the structure
anticipates it.

## What each method does

The per-step methods (`rust_arroyo.py:288` onwards) all follow one pattern:

| # | Action | Note |
| --- | --- | --- |
| 1 | Look up this step's slice of `steps_config`, by step name | |
| 2 | `override_config()`, then `validate()` | The single point where deployment config is applied — an [invariant](../contracts.md#configuration) |
| 3 | Close any open transformation chain on this route, unless the step is a map | **Required.** Skipping it emits operators out of order — see [Contracts](../contracts.md#the-adapter) |
| 4 | Translate into one or more `RuntimeOperator`s and `add_step()` them | |
| 5 | Return the route, or the per-branch routes | |

Four methods do more than the pattern:

- **`source`** creates the `ArroyoConsumer` rather than adding to one: Kafka consumer
  config, DLQ config if the source declares a DLQ stream, the Rust-side metrics config, the
  healthcheck flag and the Sentry DSN. It captures the *logical* stream name before applying
  overrides and passes that as the schema name, so a topic override does not change which
  schema messages are validated against.
- **`filter`** distinguishes a `HeadersFilter`, which becomes a fully native operator with
  no Python call, from a `PredicateFilter`, which carries a Python callable.
- **`reduce`** distinguishes `Batch`, which has a native Rust implementation, from
  everything else, which is wrapped into a Python delegate and executed by the
  [Python operator](./python-operator.md).
- **`router`** and **`broadcast`** add their operator and return one route per branch, each
  with the branch name appended as a waypoint. That is what makes the runner's traversal
  continue down every branch.

Application functions are wrapped before registration so that entering the step, its
duration and any exception are recorded — see [Metrics](./metrics.md#deliberate-symmetry-across-the-boundary).

`run()` asserts that exactly one consumer was built and calls `run()` on it, blocking in
Rust until shutdown.

## Chaining and `starts_segment`

The most interesting thing the adapter does is *not* translate steps one-to-one. The
mechanics are:

| Rule | Detail |
| --- | --- |
| **What accumulates** | Consecutive `Map` steps, in a structure keyed by route (`steps_chain.py`), so each branch accumulates independently |
| **When a chain opens** | A map arrives and no chain is open on that route, or a map is marked `starts_segment: True` |
| **When a chain closes** | Any non-map step — filter, reduce, sink, router, broadcast — closes it *before* the new step is added, which keeps operator order faithful to pipeline order |
| **What a closed chain becomes** | With no parallelism, a native `RuntimeOperator::Map` holding the fused function. With `multi_process`, a `RuntimeOperator::PythonAdapter` wrapping Arroyo's multiprocessing strategy, whose pool executes the fused function |
| **Picklability check** | The fused function is checked for picklability when the chain is finalised, *even without multiprocessing*, so a pipeline cannot work in single-process and fail when someone enables a pool in production. Build-time error, naming the offending steps |

Two consequences surprise people:

- **Parallelism can only be configured on a step that starts a segment.** Configuring it
  mid-chain is rejected: parallelism belongs to the segment, not the step.
- **A non-map step in the middle of what looks like one logical segment splits it.**
  Inserting a filter between two maps produces two chains, two operators, and potentially
  two pools. Segmentation is a consequence of the pipeline's shape as much as of the
  configuration.

Both senses of the word "segment" are defined in the
[glossary](../README.md#glossary); how they are expressed in the config file is in
[Deployment configuration](../../reference/deployment-config.md#segments).
