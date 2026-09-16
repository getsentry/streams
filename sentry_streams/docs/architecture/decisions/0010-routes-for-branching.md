# 0010 — Branching is expressed as routes on a single linear strategy chain

**Status:** accepted · **Area:** Rust runtime

## Decision

Arroyo pipelines are linear: a strategy has exactly one next strategy. The DSL has `Router`
and `Broadcast`. Rather than building a tree of strategies, every message carries a
`Route` — its source name plus the ordered waypoints of the branches it has been sent
down — and every strategy is built with the route it belongs to and forwards untouched
anything whose route differs. The chain stays physically linear and holds the steps of
every branch one after another.

Consequently the Rust adapter's stream handle **is** a `Route`: there is no stream object to
hand around, so what the runner passes to an adapter method is "which branch", and what it
gets back is the branch the output belongs to — one route per branch for router and
broadcast.

## Alternatives

- Extend Arroyo with real branching strategies that own several next steps.
- One consumer process per branch.

## Rationale

Keeping a single arroyo chain of steps simplified considerably commit management
to guarantee at least once delivery.

Having one single chain ensures that there is a single last step that receives
messages from all branches and can evaluate which offsets have to be committed.

## Consequences

- A message physically traverses every strategy in the consumer, including branches it will
  never enter. This is affordable only because the route check is cheap and GIL-free, which
  makes "check the route first, forward on mismatch, take no GIL" the first rule for every
  step author.
- Commit correctness no longer follows from reaching the end of the chain, which is what
  [0011](./0011-watermarks-drive-commits.md) exists to fix.
- More than one consumer per process is not supported today, though the adapter's structure
  anticipates it.

## See also

[Routes](../rust-arroyo/messages.md#routes) ·
[The stream handle is a route](../rust-arroyo/adapter.md#the-stream-handle-is-a-route)
