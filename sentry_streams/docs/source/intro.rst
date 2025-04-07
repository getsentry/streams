Sentry Streams is a distributed platform that, like most streaming platforms,
is designed to handle real-time unbounded data streams.

This is built primarily to allow the creation of Sentry ingestion pipelines
though the api provided is fully independent from the Sentry product and can
be used to build any streaming application.

The main features are:

* Kafka sources and multiple sinks. Ingestion pipeline take data from Kafka
  and write enriched data into multiple data stores.

* Dataflow API support. This allows the creation of streaming application
  focusing on the application logic and pipeline topology rather than
  the underlying dataflow engine.

* Support for stateful and stateless transformations. The state storage is
  provided by the platform rather than being part of the application.

* Distributed execution. The primitives used to build the application can
  be distributed on multiple nodes by configuration.

* Hide the Kafka details from the application. Like commit policy and topic
  partitioning.

* Out of the box support for some streaming applications best practices:
  DLQ, monitoring, health checks, etc.

* Support for Rust and Python applications.

* Support for multiple runtimes.

Design principles
=================

This streaming platform, in the context of Sentry ingestion, is designed
with a few principles in mind:

* Fully self service to speed up the time to reach production when building pipelines.
* Abstract infrastructure aspect away (Kafka, delivery guarantees, schemas, scale, etc.) to improve stability and scale.
* Opinionated in the abstractions provided to build ingestion to push for best practices and to hide the inner working of streaming applications.
* Pipeline as a system for tuning, capacity management and architecture understanding

Getting Started
=================

In order to build a streaming application and run it on top of the Sentry Arroyo
runtime, follow these steps:

1. Run locally a Kafka broker.

2. Create a new Python project and a dev environment.

3. Import sentry streams

.. code-block::

    pip install sentry_streams


4. Create a new Pyhon module for your streaming application:

.. code-block:: python
    :linenos:

    from json import JSONDecodeError, dumps, loads
    from typing import Any, Mapping, cast

    from sentry_streams.pipeline import Filter, Map, streaming_source

    def parse(msg: str) -> Mapping[str, Any]:
        try:
            parsed = loads(msg)
        except JSONDecodeError:
            return {"type": "invalid"}

        return cast(Mapping[str, Any], parsed)


    def filter_not_event(msg: Mapping[str, Any]) -> bool:
        return bool(msg["type"] == "event")

    pipeline = (
        streaming_source(
            name="myinput",
            stream_name="events",
        )
        .apply("mymap", Map(function=parse))
        .apply("myfilter", Filter(function=filter_not_event))
        .apply("serializer", Map(function=lambda msg: dumps(msg)))
        .sink(
            "myoutput",
            stream_name="transformed-events",
        )
    )

This is a simple pipeline that takes a stream of JSON messages, parses them,
filters out the ones that are not events, and serializes them back to JSON
and produces the result to another topic.

5. Run the pipeline

.. code-block::

    SEGMENT_ID=0 python -m sentry_streams.runner \
    -n Batch \
    --config sentry_streams/deployment_config/<YOUR CONFIG FILE>.yaml \
    --adapter arroyo \
    <YOUR PIPELINE FILE>


6. Produce events on the `events` topic and consume them from the `transformed-events` topic.

.. code-block::

    echo '{"type": "event", "data": {"foo": "bar"}}' | kcat -b localhost:9092 -P -t events

.. code-block::

    kcat -b localhost:9092 -G test transformed-events


7. Look for more examples in the `sentry_streams/examples` folder of the repository.
