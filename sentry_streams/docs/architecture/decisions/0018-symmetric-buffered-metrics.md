# 0018 — Metrics are symmetric across the language boundary and buffered, not sampled

## Decision

Step-level metrics are produced by an equivalent buffering component on each side of the
language boundary — `PipelineStats` in Python, thread-local state in Rust — with the same
semantics, the same metric names, the same `step` tag and the same ten-second flush
interval. The adapter wraps every application function it registers so entry, duration and
exceptions are recorded around the call.

Aggregation is by buffering, not sampling.

## Alternatives

- Emit each metric as it happens.
- Sample in hot paths.
- Let each side report in whatever shape suits it.

## Rationale

Buffering is critical for metrics. Initially we were producing metrics at each
step, that ended up representing more than 90% of the CPU usage of the application.

Moreover, as we process tens of thousands of messages per second per consumer
and we process each of them in multiple steps, even the data structure to
accumulate metrics is critical.

PipelineStats is intentionally bare bone. Making it heavier can have disproportionate
effect.

## Consequences

- A step reports identically whether it was fused into a native operator or executed as a
  Python function, so one set of dashboards reads a pipeline end to end. A new Rust strategy
  is expected to keep that property.
- Rare events are kept rather than discarded, which sampling would not guarantee, and metrics
  produced in tight loops do not cost a significant fraction of the work being measured.
- Metrics are delayed by up to the flush interval.
- Worker processes inherit nothing, so the pool initializer reconfigures metrics in each one
  — which requires the config to be plain data rather than a live backend object
  (an [invariant](../contracts.md#configuration)).

## See also

[Metrics](../rust-arroyo/metrics.md)
