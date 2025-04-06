# streams

The Sentry Streaming Platform

Sentry Streams is a distributed platform that, like most streaming platforms,
is designed to handle real-time unbounded data streams.

This is built primarily to allow the creation of Sentry ingestion pipelines
though the api provided is fully independent from the Sentry product and can
be used to build any streaming application.

The main features are:

- Kafka sources and multiple sinks. Ingestion pipeline take data from Kafka
  and write enriched data into multiple data stores.

- Dataflow API support. This allows the creation of streaming application
  focusing on the application logic and pipeline topology rather than
  the underlying dataflow engine.

- Support for stateful and stateless transformations. The state storage is
  provided by the platform rather than being part of the application.

- Distributed execution. The primitives used to build the application can
  be distributed on multiple nodes by configuration.

- Hide the Kafka details from the application. Like commit policy and topic
  partitioning.

- Out of the box support for some streaming applications best practices:
  DLQ, monitoring, health checks, etc.

- Support for Rust and Python applications.

- Support for multiple runtimes.

[Streams Documentation](https://getsentry.github.io/streams/)
