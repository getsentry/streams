# 0014 — Consecutive maps are fused into a chain; parallelism is a property of the segment

**Status:** accepted · **Area:** Rust runtime

## Decision

The adapter does not translate steps one to one. Consecutive `Map` steps accumulate into a
chain, kept per route so each branch accumulates independently. Any non-map step closes the
open chain before it is added, keeping operator order faithful to pipeline order. A closed
chain becomes a single operator: a native `RuntimeOperator::Map` holding the fused function,
or — when the segment is configured with `multi_process` — a `PythonAdapter` wrapping
Arroyo's multiprocessing strategy, whose pool executes the fused function.

Parallelism is configured only on a step that starts a segment; configuring it mid-chain is
rejected. The fused function is checked for picklability when the chain is finalised, **even
when multiprocessing is off**.

## Alternatives

- One operator per step, with a GIL acquisition and a message hop each.
- Check picklability only when a pool is actually configured.

## Rationale

This is meant to avoid going back and forth between Rust and python when the
back and forth is not needed. Moving messages around requires taking and releasing
the GIL, it is not the most efficient operation.

It is also critical when we run steps across multiple processes as each parallel
step would need to have its own shared memory and processing pool. Chaining
stateless operations is a no brainer there.

## Consequences

- A run of maps costs one boundary crossing instead of one per step.
- A non-map step in the middle of what looks like one logical segment splits it: inserting a
  filter between two maps produces two chains, two operators and potentially two pools.
  Segmentation follows the pipeline's shape as much as the configuration.
- A pipeline cannot pass in a single-process deployment and then fail when someone enables a
  pool in production; unpicklable functions are a build-time error naming the offending
  steps.
- "Segment" means two different things — a config-list entry (one Kubernetes workload) and a
  fusion boundary within a process. Both are in the [glossary](../README.md#glossary).

## See also

[Chaining and starts_segment](../rust-arroyo/adapter.md#chaining-and-starts_segment) ·
[The adapter](../contracts.md#the-adapter) ·
[The multiprocess step](../rust-arroyo/python-operator.md#the-multiprocess-step)
