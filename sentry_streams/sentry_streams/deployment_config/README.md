# Configuration files

Configuration files have two main sections right now; `env` and `pipeline`. These files are also
adapter/runtime-specific right now.

`env` is supposed to be where all general environment config can go. For example, in the
case of a Flink configuration file, that could mean setting certain properties that hold true
for an entire streaming pipeline.

`pipeline` holds configuration for each segment. A segment is defined as a set of steps or operators
which will be executed on one unit (this could be a worker, a single consumer, or a Flink slot). Each
segment can have a parallelism override if parallelism is a value other than 1. The segment also holds
step-specific configuration as a mapping. This step config should hold values that are overrides of defaults.
As of now, the only steps that need specific configuration are sources and sinks. Thus, segments which contain
either a source or sink must have the config and any overrides specified.

The idea is that, using segments and parallelism, we can achieve distribution of steps across different
physical workers. With a runtime like Flink, we can create segments (also known as chains in Flink terms)
for different parts of the pipeline, and give each segment different parallelism values. With this, we
could have, for example, a segment which lives in one worker, reshuffling data to another segment which
is distributed across 10 workers.

For now, step-specific defaults (for settings like `auto-offset-reset`) are embedded into each adapter,
but ultimately these should live in configuration.

See `config.json` for the current schema of a configuration file and `config_types.py` for concrete types
that the configuration gets converted into.

See example configuration files in this directory.
