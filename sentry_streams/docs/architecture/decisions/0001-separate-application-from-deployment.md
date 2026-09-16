# 0001 — An application describes a dataflow; a separate config describes its deployment


## Decision

A streaming application is a pure, declarative description of a dataflow graph: a source, a
series of transformations, one or more sinks. It contains no broker address, no consumer
group, no topic, no parallelism setting and no reference to the runtime that will execute
it. Everything the description leaves out lives in a separate deployment configuration
file, which addresses each step by its name.

The split is a hard line, not a convention: only non-semantic values are overridable
(see [Configuration](../contracts.md#configuration)), and step names are the only coupling
between the two artefacts.


## Rationale

The decision is meant to simplify the separation between infrastructure and application
which is needed to build a streaming platform.

Everything in the config file is specific to how the pipeline is operated:

* Different by environment (Kafka hosts)

* Scale configuration

* Optimizations

Separating these concepts allows the product engineers to focus on how the
application works while platform engineers can ensure the application runs in
a scalable and performant way.

Enforcing a separation ensures also that platform engineers would have the same
parameters to tune for all pipelines independently on what the application
does.

## Consequences

- The same application file can be deployed against different brokers, topics, parallelism
  settings and even different runtimes without being modified.
- Renaming a step is a breaking change to its deployment configuration.
- Anything a deployment needs to vary must be modelled as an overridable key on a step;
  there is no escape hatch.

## See also

[The model](../README.md#the-model) · [Pipeline DSL and runner](../pipeline-dsl-and-runner.md) ·
[Deployment configuration](../../reference/deployment-config.md)
