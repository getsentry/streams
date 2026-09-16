# 0004 — Higher level steps are sugar that converts to primitives, with a native override hook

## Decision

Recurring combinations of primitives — `Parser`, `Serializer`, `BatchParser`,
`ParquetSerializer`, `Reducer` — are `ComplexStep`s. Each implements `convert()`, returning
plain simple steps, usually a `Map` with a function partial-applied. The translator calls
`convert()` transparently, so by the time an adapter sees the pipeline a `Parser` is just a
`Map`.

An adapter that has a faster native implementation of a complex step declares it through
`complex_step_override()` and receives the original step instead of its conversion.
Adapters that do not care return an empty mapping.

## Alternatives

- Make every useful step a primitive, and require every adapter to implement it.
- Keep them as plain helper functions in application code.

## Rationale

There are many common variants of basic steps that customers would have to implement
on their own starting from basic building block.

If we made a primitive for each of them we would have to implement them on each
runtime in depth.

If we asked product teams to implement each we would have a lot of cases where
we reinvent the wheel.

This allows us to provide composite steps quickly without the overhead of
implementing them in each adapter.


## Consequences

- The set of operations an adapter must implement stays small.
- Complex steps are the first thing to reach for when extending the DSL; only what cannot be
  composed becomes a primitive.
- A complex step's per-step configuration and metrics are attributed to the step it converts
  into, unless an adapter overrides it natively.

## See also

[Complex steps](../pipeline-dsl-and-runner.md#complex-steps) ·
[Decide what you are actually adding](../guides/adding-a-step.md#0-decide-what-you-are-actually-adding)
