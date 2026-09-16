# Decision log

One page per architectural decision that shaped the platform. Each page states the
decision, what it rules out, and the consequences we live with. The **rationale** section
is where the reasoning behind the decision is recorded — why this option, at the time, over
the alternatives.

These are records, not proposals. A decision that is later reversed keeps its page and gains
a superseded status, so the history of why the system looks like it does stays readable.

The mechanics of each decision live in the architecture pages; a decision page links to them
rather than repeating them.

## The platform shape

| # | Decision |
| --- | --- |
| [0001](./0001-separate-application-from-deployment.md) | An application describes a dataflow; a separate config describes its deployment |
| [0002](./0002-adapters-as-the-runtime-boundary.md) | Runtimes are pluggable adapters with one method per primitive |
| [0004](./0004-complex-steps-as-sugar.md) | Higher level steps are sugar that converts to primitives, with a native override hook |
| [0005](./0005-statically-typed-dsl.md) | The DSL is statically typed end to end |

## The Rust runtime

| # | Decision |
| --- | --- |
| [0009](./0009-rust-runs-python-builds.md) | Rust owns the runtime loop; Python builds the pipeline |
| [0010](./0010-routes-for-branching.md) | Branching is expressed as routes on a single linear strategy chain |
| [0011](./0011-watermarks-drive-commits.md) | Commits are driven by periodic watermarks, not by data messages |
| [0013](./0013-embed-python-strategies-via-a-pull-delegate.md) | Existing Python Arroyo strategies are embedded through a pull-based delegate, not reimplemented |
| [0014](./0014-fuse-maps-into-segments.md) | Consecutive maps are fused into a chain; parallelism is a property of the segment |

## Observability

| # | Decision |
| --- | --- |
| [0018](./0018-symmetric-buffered-metrics.md) | Metrics are symmetric across the language boundary and buffered, not sampled |

## Writing a new one

Copy the shape of an existing page: a one-line status, **Decision**, **Alternatives**,
**Rationale**, **Consequences**, **See also**. Keep the decision to a few sentences and put
the reasoning in the rationale. Number sequentially; never renumber an existing page.
