# Contracts and invariants

Rules that hold across the platform. Each one is load-bearing: something breaks, usually
subtly, if it stops holding. Use this as a review checklist.

Behavioural guarantees to the outside world — delivery semantics, failure policy — are in
[Guarantees](./guarantees.md).

## The runner

| Invariant | Why |
| --- | --- |
| Loading the runtime and running it are separate functions | The Rust CLI embeds a Python interpreter, calls the loading function to obtain the built runtime, and calls `run()` itself from Rust. Merging them breaks that entry point |
| Exceptions from product code propagate | A Sentry SDK initialised by the application has to be able to capture them |
| Every branch terminates in a sink, validated before anything is built | A dangling branch is nearly always a bug; catching it after startup is far more expensive |

## Configuration

| Invariant | Why |
| --- | --- |
| `override_config()` then `validate()`, called by the adapter at translation time, and nowhere else | Before it, the step holds what the application wrote; after it, what this deployment wants. A step valid as written may be invalid once overridden, so validation must follow the override |
| No other component reads config keys off a step | One place to look when a value is wrong |
| Configuration is plain data, re-applied per process | It has to survive being pickled into a multiprocessing worker. A live backend object would not |
| Only non-semantic values are overridable | Functions, graph shape and types belong to the application; topics, brokers, consumer groups, batch sizes and parallelism belong to the deployment |

## The adapter

| Invariant | Why |
| --- | --- |
| The fused function of a closed chain is checked for picklability, even when multiprocessing is off | Otherwise a pipeline works in a single-process deployment and fails when someone enables a pool in production |
| Parallelism is configured only on a step that starts a segment | Parallelism belongs to the segment; a step in the middle of a chain is not its own execution unit |
| Nothing executes during the build phase | The graph traversal only accumulates; the runtime is assembled in `run()` |
| Operator descriptors are kept, not consumed | The strategy chain is rebuilt from them on every rebalance |

## The message model

| Invariant | Why |
| --- | --- |
| A strategy checks the route first and forwards untouched on mismatch, without taking the GIL | Every message physically traverses every strategy; this is what makes that affordable |
| A strategy forwards watermarks unless it buffers messages, in which case it holds them until the buffered work is out | A committed offset must be a processed offset |
| A `PyWatermark` is never submitted back into a Python operator | Directional conversion invariant of `WatermarkMessage` |
| A `PyWatermark` is never handed to the Kafka sink | Same |
| A step handles every payload variant: supports what it can, panics loudly on what it cannot | Silently forwarding an unsupported payload corrupts data downstream instead of failing at the bug |
| Payloads are not converted unless they are read | An opaque payload is a pointer move; a converted one is a copy plus a GIL acquisition |
| Messages are immutable; replacing a payload produces a new message | Enforced by Rust, a convention in Python |

## Extending the DSL

Adding a primitive is a **breaking change to every adapter**, by construction: the adapter
interface has one abstract method per primitive, and `RuntimeTranslator` dispatches on a
closed `StepType` enum. That is a deliberate trade.

What it obliges you to do:

1. Every adapter must implement the new method. An adapter that will not support the
   primitive raises `NotImplementedError` — that is the sanctioned way to decline, and is
   how `flat_map` is handled today in the Rust adapter.
2. `RuntimeTranslator.translate_step` must gain a branch, or `assert_never` fails the type
   check.
3. If the primitive is not a map, its adapter methods must close the chain (see above).
4. Prefer a `ComplexStep` if the behaviour can be composed from existing primitives. It
   costs no adapter changes and adapters may still override it natively.

The procedure is in [Adding a step](./guides/adding-a-step.md).
