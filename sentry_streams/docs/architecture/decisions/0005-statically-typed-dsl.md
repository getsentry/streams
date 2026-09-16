# 0005 — The DSL is statically typed end to end

## Decision

`Pipeline[TOut]` carries the type of the messages flowing out of the last step added, and
every primitive is generic in its input and output types. Chaining a step whose input type
does not match the pipeline's current output type is a type error, caught by mypy in strict
mode rather than at runtime.

## Alternatives

- Untyped payloads, with mismatches surfacing as runtime failures in the consumer.
- Runtime schema checks at step boundaries instead of static types.

## Rationale

This is meant to reduce the chances of runtime errors though it has a cost:
errors are not very easy to spot.

We should still consider validating the types at startup or during tests.

## Consequences

- Structural mistakes are caught before deployment — for instance sinking parsed objects
  into a Kafka sink with no serializer in between, which would otherwise panic at runtime
  (see [Messages](../rust-arroyo/messages.md#conversion-is-limited-on-purpose)).
- Every new step class must declare its generic parameters correctly for the chain to keep
  type checking.
- Rust `pyclass` types cannot be made generic to the type checker, which is one reason
  Python code sees the `Message` wrappers rather than the Rust types.

## See also

[Principles](../pipeline-dsl-and-runner.md#principles) ·
[Declare the step in the DSL](../guides/adding-a-step.md#2-declare-the-step-in-the-dsl)
