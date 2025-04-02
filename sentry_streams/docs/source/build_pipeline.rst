Building a Pipeline
===================

Pipelines are defined through a python DSL (more options will be provided) by
chaining dataflow primitives.

Chaining primitives means sending a message from one operator to the following
one.

Pipelines start with `StreamingSource` which represent a Kafka consumer. They
can fork and broadcast messages to multiple branches. Each branch terminates
with a Sink.

As of now only Python operations can be used. Soon we will have Rust as well.

Distribution is not visible at this level as it only defines the topology of
the application, which is basically its business logic. The distribution is
defined via the deployment descriptor so the operators can be distributed
differently in different environments.

The DSL operators are in the `chain.py` module.

.. automodule:: sentry_streams.pipeline.chain
   :members:
   :undoc-members:
   :show-inheritance:
