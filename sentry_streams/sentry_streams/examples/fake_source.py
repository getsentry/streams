"""
Profiling pipeline backed by a fake (Kafka-less) source.

Instead of reading from Kafka, the source synthesises messages of random bytes
at a fixed rate and terminates after a fixed number of messages. This is useful
to profile the pipeline machinery (the Python/Rust step execution) without
standing up a Kafka broker.

Run it with the ``rust_arroyo`` adapter, e.g.::

    python -m sentry_streams.runner --adapter rust_arroyo \
        --segment-id 0 \
        --config sentry_streams/deployment_config/fake_source.yaml \
        sentry_streams/examples/fake_source.py

The consumer stops on its own once all messages have been produced.
"""

from sentry_streams.examples.transform_metrics import noop
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Map,
    fake_streaming_source,
)

pipeline = fake_streaming_source(
    name="fake_source",
    message_size_bytes=1024,
    messages_per_second=1000.0,
    num_messages=10000,
)

(
    pipeline.apply(Map("noop", function=noop)).sink(
        DevNullSink[bytes](
            name="devnull",
            batch_size=500,
            batch_time_ms=5000.0,
        )
    )
)
