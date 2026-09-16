# Metrics

**Audience:** everyone · **Durability:** mechanism · **Last reviewed:** 2026-09-15

A running pipeline emits metrics from both sides of the language boundary and from several
independent producers within each. They are configured once and all end up in the same
place under the same namespace.

How to configure them is in
[Deployment configuration](../../reference/deployment-config.md#metrics). This page covers
only what is architecturally load-bearing.

## What produces metrics

| Producer | Side | What it reports |
| --- | --- | --- |
| Pipeline stats | Python | Per-step message counts, errors, durations, for steps executed in Python |
| Pipeline stats | Rust | The same, for steps executed natively |
| Rust strategies | Rust | Operator internals: Python adapter submit/poll durations, batch sizes, commit latency |
| Arroyo (Python) | Python | The Python Arroyo library's own consumer metrics |
| Arroyo (Rust) | Rust | The Rust Arroyo library's own consumer metrics |

All are namespaced under `streams.pipeline`. Tags carry at least the pipeline name, and
per-step metrics carry the step name, so a dashboard can break a pipeline down by step
regardless of which side of the boundary the step ran on.

## Deliberate symmetry across the boundary

Step-level metrics are handled by an equivalent buffering component on each side, with the
same semantics and the same metric names:

- In Python, `PipelineStats` accumulates counters and timings per step and flushes every ten
  seconds. The adapter wraps every application function it registers, so counts and timings
  are recorded around the call, including when it raises.
- In Rust, the same buffering exists with thread-local state (`src/pipeline_stats.rs`),
  flushed on the same interval, emitting the same names with the same `step` tag.

The point of the symmetry is that a step reports identically whether it was fused into a
native operator or executed as a Python function, so one set of dashboards reads a pipeline
end to end. A new Rust strategy is expected to keep that property — see
[Adding a step](../guides/adding-a-step.md#6-instrument-it).

Buffering rather than sampling is deliberate: some metrics are produced in tight loops where
emitting costs a significant fraction of the work being measured, and aggregation keeps rare
events instead of discarding them.

Beyond step stats, the Rust strategies emit their own internals, which is where to look when
a pipeline is slow and step timings do not explain it: how long a `submit` into a Python
delegate took, how long the delegate's `poll` took, how long the next strategy took to
accept the result, and end-to-end consumer latency measured at commit time from the
timestamp a watermark carries.

## Multiprocessing

Worker processes are separate interpreters and inherit nothing. The pool is created with an
initializer that calls `configure_metrics` again, with the same config, in each worker.

This is why the metrics configuration is a plain dictionary rather than a live backend
object: it has to survive being pickled and sent to a worker. The broader rule — config is
data, re-applied per process, never passed by reference — is an
[invariant](../contracts.md#configuration).
