# Guarantees and failure modes

What the platform promises, and how it behaves when things go wrong. This is the page to
read before an incident, not during one.

## Delivery semantics

**At-least-once.** Offsets are committed only after the work they represent has been done:
the commit policy sits at the end of the strategy chain and commits the offsets a watermark
carries once that watermark has arrived from every branch. A crash between processing and
commit therefore replays. There is no deduplication and no transactional producer, so
duplicates are visible to sinks.

Consequences worth stating plainly:

- A Kafka sink may produce the same record more than once. This is not actually
  working today.
- A GCS sink may write the same object more than once.
- Application steps must tolerate being run twice on the same input.


## Failure policy

The runtime distinguishes three kinds of failure, deliberately:

| Kind | Trigger | Behaviour |
| --- | --- | --- |
| **Invalid message** | A Python step raises `InvalidMessageError` (`pipeline/exception.py`); a delegate raises Arroyo's `InvalidMessage` | Offset and partition are read from the message and it is routed to the DLQ |
| **Backpressure** | A delegate raises `MessageRejected` | The message is handed back to the caller to be retried; pressure propagates upstream |
| **Bug** | Any other exception from a Python step; an unsupported payload variant reaching a step; a `PyAnyMessage` reaching the Kafka sink | Panic. Continuing would mean silently dropping or corrupting data |

Two important qualifications:

- **With no DLQ configured, an invalid message stops processing.** The consumer logs
  `DLQ not configured, invalid messages will cause processing to stop` at startup. A DLQ is
  configured per source via the `dlq` key.
- **An invalid message on an `AnyMessage` panics** rather than going to the DLQ, because
  there is no offset to attribute it to. Messages produced downstream of the Python operator
  are any-messages, which is why a failure there cannot be DLQ'd.

DLQ limits are currently Arroyo's defaults — no rate limit and no cap on buffered messages.
That is a known gap, not a deliberate decision.

## Backpressure

Backpressure is Arroyo's: a strategy that cannot accept a message returns it, and the
processor stops polling until it can make progress. The Python operator participates
properly — `MessageRejected` from a delegate becomes Arroyo backpressure, and the drain of
`poll` results stops at the first rejection, preserving order.

There is no load shedding and no queue with a bounded drop policy. A pipeline that cannot
keep up lags; it does not lose data.

## Rebalance and shutdown

- On **shutdown** (SIGINT/SIGTERM) the processor handle signals shutdown and the main loop
  exits. `shutdown()` on the Python adapter is not implemented; shutdown is driven from Rust.
- On **rebalance**, Arroyo tears down and rebuilds the strategy chain. This is why the
  consumer keeps `RuntimeOperator` descriptors rather than consuming them, and why delegates
  are created by a factory: state too expensive to rebuild — notably the multiprocessing
  pool — lives in the factory and survives.
- **Unverified:** behaviour when a multiprocessing worker dies mid-batch.

## Health

When `write_healthcheck` is enabled in the adapter config, the `HealthCheck` strategy
touches a file on every poll, which the Kubernetes liveness probe reads. The signal means
"the main loop is turning"; it does not mean the pipeline is making progress, and it will
keep succeeding while the consumer is backpressured.
