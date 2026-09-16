# 0011 — Commits are driven by periodic watermarks, not by data messages

## Decision

A `WatermarkEmitter` at the head of the chain records the offsets of every message passing
through it and, every few seconds (10 by default), injects a watermark control message
carrying the accumulated committable. Watermarks travel down the pipeline like data
messages and go through the same branching logic. At the end of the chain the commit policy
ignores data messages entirely: it counts watermark copies, and commits the carried offsets
only once it has seen that watermark arrive from every branch.

Steps that hold messages back hold watermarks with them; every other step forwards them.

## Alternatives

- Arroyo's default policy: commit a message's offsets when it reaches the end of the chain.
- Track every in-flight offset and its per-branch completion.

## Rationale

Watermarks make it much easier to calculate the offset to commit and when to commit
in scenarios where the pipeline has branches.

The last step of the pipeline receives all the watermarks propagated through all
branches and is able to ensure that all branches reached a specific offset
before committing.

This also allows us to drop messages (via filters for example) and it allows each
step to decide when it is done processing up a certain offsets just by holding
the offsets instead of propagating them.

## Consequences

- With a broadcast in the pipeline, committing on the first copy to arrive would commit work
  that has not happened; counting copies is what makes at-least-once hold under branching.
- The bookkeeping is bounded: a handful of in-flight watermarks rather than a set of
  in-flight offsets. Trackers that never receive all their copies are dropped after a
  timeout.
- Commit latency is bounded below by the watermark interval, not by message throughput.
- Every step author must handle watermarks explicitly, and buffering steps must not let them
  overtake the data.
- A watermark crossing into Python becomes a `PyWatermark`, with directional invariants:
  never submitted back into a Python operator, never handed to the Kafka sink.
- Watermarks carry the newest data message timestamp, which is how end-to-end consumer
  latency is measured at commit time.

## See also

[Watermarks](../rust-arroyo/messages.md#watermarks) ·
[The message model](../contracts.md#the-message-model)
