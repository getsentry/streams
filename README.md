# streams

The Sentry Streaming Platform

This repo contains two libraries: `sentry_streams` and `sentry_flink`.

The first contain all the streaming api and an Arroyo based adapter to run
the streaming applications on top of Arroyo.

The second contains the Flink adapter to run streaming applications on
Apache Flink. This part is in a separate library because, until we will not
be able to make it run on python 3.13 and produce wheels for python 3.13,
it will require Java to run even in the dev environment.

## Quickstart

We are going to run a streaming application on top of Arroyo.

1. Run `make install-dev`

2. Go to the sentry_streams directory

3. Activate the virtual environment: `source .venv/bin/activate`

4. Run one of the examples

```
python sentry_streams/runner.py \
    -n test \
    -b localhost:9092 \
    -a arroyo \
    sentry_streams/examples/transfomer.py
```

This will start an Arroyo consumer that runs the streaming application defined
in `sentry_streams/examples/transfomer.py`.

there is a number of examples in the `sentry_streams/examples` directory.
