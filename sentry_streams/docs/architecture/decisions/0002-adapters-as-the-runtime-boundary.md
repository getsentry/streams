# 0002 — Runtimes are pluggable adapters with one method per primitive

## Decision

The execution runtime sits behind an adapter interface with one abstract method per
primitive (`source`, `map`, `filter`, `reduce`, `sink`, `router`, `broadcast`, `flat_map`).
The interface is generic in the type of the stream handle, so an adapter represents "a
stream" however its runtime requires, and the runner never inspects it. Dispatch goes
through `RuntimeTranslator` over a closed `StepType` enum ending in `assert_never`.

Adding a primitive is therefore a **breaking change to every adapter**, by construction. An
adapter that will not support a primitive declines explicitly with `NotImplementedError`,
as the Rust adapter does for `flat_map`.


## Rationale

At the time of writing the team is not settled on which runtime should be used
at scale. This abstraction layer allows the streaming team to experiment with
different runtimes while not breaking existing applications.

Having one method per primitive is meant to reduce the complexity of the
adapter.

## Consequences

- A new primitive cannot be added silently: the type checker fails until every adapter and
  the translator handle it.
- Pressure toward [complex steps](./0004-complex-steps-as-sugar.md), which cost no adapter
  changes.
- The runner stays thin; runtime knowledge lives entirely in the adapter.

## See also

[Extending the DSL](../contracts.md#extending-the-dsl) ·
[Adding a step](../guides/adding-a-step.md) ·
[From pipeline to consumer](../pipeline-dsl-and-runner.md#from-pipeline-to-consumer)
