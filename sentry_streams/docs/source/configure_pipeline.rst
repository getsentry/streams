Pipeline runtime configuration
==============================

The pipeline definition described in :doc:`Building a Pipeline <build_pipeline>`
is a logical view on the streaming pipeline.  It does not include any
detail on how to run the code or how to configure the pipeline on
specific environment.

The configuration is managed by a YAML config file associated with the
streaming pipeline itself.

The config file specifies details like:

* Distribution and parallelism
* External resources configuration (like Kafka Brokers)
* Tuning configuration
* Runtime specific configuration to keep the pipeline agnostic

Pipeline-level runtime options
-----------------------------

Under ``pipeline`` you can set optional ``adapter_config`` to control adapter
behavior. For the Arroyo/Rust Arroyo adapter:

* ``adapter_config.arroyo.write_healthcheck`` (boolean, default false): when true,
  the consumer touches a file (by default ``/tmp/health.txt``) on each poll so that
  Kubernetes liveness probes can detect a blocked main thread. See the
  `Arroyo healthcheck strategy <https://github.com/getsentry/arroyo/blob/main/docs/source/strategies/healthcheck.rst>`_
  for probe configuration (e.g. ``periodSeconds`` should be higher than
  ``max.poll.interval.ms``).

The configuration is meant to rely on sensible defaults. Most parameters
are supposed to have a default. Only overrides need to be specified in
the configuration.

Example

.. code-block:: yaml

    env: {}

    pipeline:
      adapter_config:
        arroyo:
          write_healthcheck: true   # optional; for Kubernetes liveness
      segments:
        - steps_config:
            myinput:
                starts_segment: True
                bootstrap_servers: ["127.0.0.1:9092"]
                parallelism: 1
            parser:
                starts_segment: True
                parallelism:
                    multi_process:
                    processes: 4
                    batch_size: 1000
                    batch_time: 0.2
            mysink:
                starts_segment: True
                bootstrap_servers: ["127.0.0.1:9092"]

Environment variable overrides
-----------------------------

You can override **literal (string) fields** in the config using the
``${envvar:VAR_NAME}`` syntax. The value is replaced with the environment
variable at load time.

* **Syntax**: Set a field to ``${envvar:VAR_NAME}`` (e.g. ``max.poll.interval.ms: "${envvar:MAX_POLL_INTERVAL_MS}"``).
  Only string values are substituted; structured fields (nested objects) cannot
  be replaced by an env var.
* **Missing variable**: If a referenced environment variable is not set, the
  pipeline fails at startup with a clear error that includes the variable name.

Example with env var for a list entry:

.. code-block:: yaml

    pipeline:
      segments:
        - steps_config:
            myinput:
              starts_segment: true
              bootstrap_servers: ["127.0.0.1:9092"]
              override_params:
                max.poll.interval.ms: "${envvar:MAX_POLL_INTERVAL_MS}"

Run with the variable set, e.g. ``MAX_POLL_INTERVAL_MS=30000``.


Distribution and parallelism
============================

Distribution and parallelism are not pipeline definition concerns. They are
pipeline configuration concerns for several reasons:

* Different runtimes support different models
* Deploying the pipeline at different scale requires different distribution
  configuration.
* Even with a single runtime, there are generally different ways to parallelize
  workloads (threads, processes). Most of the times there is no reason to make
  a change to the application in order to switch from a model to another one.

As an example: Flink allows distribution by deploying chains of operators on
multiple nodes. Arroyo instead only supports distribution by deploying multiple
replicas of the whole pipeline and parallelism via threads and processes.

Segments
--------

The main logical distribution concept is the `segment`. A `segment` represent
a chunk of sequential steps of the pipeline that share distribution and parallelism
configuration.

* A segment can contain branches.
* A segment is a logical concept. It can be implemented and distributed differently
  on each Runtime.
* The application can specify how data is logically repartitioned between segments.
  Some runtime may not support all repartitioning modes.

.. mermaid::

    flowchart LR
        source --> segment1
        segment1 -- re-partition --> segment2
        segment2 --> sink

        subgraph segment1 [Segment 1 - parallelism = 2]
            direction LR
            m1[Map 1] --> m2[Filter 1]
            m3[Map 1] --> m4[Filter 1]
        end

        subgraph segment2 [Segment 2 - parallelism = 3]
            direction LR
            m11[Map 2]
            m12[Map 2]
            m13[Map 2]
        end

The diagram above could be achieved with a configuration like this

.. code-block:: yaml

    pipeline:
    segments:
        - steps_config:
            source:
                starts_segment: True
                parallelism: 1
            "Map 1":
                starts_segment: True
                parallelism:
                    multi_process:
                        processes: 2
                        batch_size: 1000
                        batch_time: 0.2
            "Map 2":
                starts_segment: True
                parallelism:
                    multi_process:
                        processes: 3
            sink:
                starts_segment: True
                parallelism:
                    threads: 1

A segment is defined by marking a step in the configuration with the
step with `starts_segment: True`.
All the following steps are chained to the new segment. This approach
allows us to minimize the boilerplate in the config file. Only the
steps that impact distribution need to be configured.

There are multiple types of parallelism and distribution configuration.
In the example multi_process is shown. That's the only one implemented
as of now.
Not all modes are supported by all runtimes.

The parallelism factor (processes number in the multi process system) are
considered absolute and not relative to the previous segment.
In Arroyo, if we deploy 2 replicas and define a step with parallelism = 4,
each replica will have 2 processes.
