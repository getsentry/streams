"""
In-process integration test for the fake (Kafka-less) source.

This builds the RustArroyoAdapter the same way the runner does and calls
``run()`` directly in the test process. The fake consumer synthesises a fixed
number of messages and then terminates on its own, so ``run()`` returns without
needing a Kafka broker or an external signal.

Note: ``ArroyoConsumer.run()`` initialises global process state (tracing and the
Ctrl+C handler) exactly once, so this module keeps to a single ``run()`` call.
"""

from __future__ import annotations

from typing import List

from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Map,
    fake_streaming_source,
)
from sentry_streams.runner import iterate_edges

NUM_MESSAGES = 20
MESSAGE_SIZE_BYTES = 256

# Collects every payload that flows through the Map step. Module-level so the
# Rust runtime can call into it and the test can inspect it afterwards.
RECEIVED: List[bytes] = []


def collect(msg: Message[bytes]) -> bytes:
    RECEIVED.append(msg.payload)
    return msg.payload


def test_fake_source_end_to_end() -> None:
    RECEIVED.clear()

    pipeline = fake_streaming_source(
        name="fake_source",
        message_size_bytes=MESSAGE_SIZE_BYTES,
        # High rate so the test completes quickly.
        messages_per_second=10000.0,
        num_messages=NUM_MESSAGES,
    ).apply(Map("collect", function=collect))
    pipeline.sink(DevNullSink[bytes](name="devnull"))

    adapter = RustArroyoAdapter.build(
        {
            "steps_config": {
                "fake_source": {"starts_segment": True},
                "devnull": {},
            },
        },
        {"type": "dummy"},
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Blocks until the fake consumer has produced all messages and self-terminates.
    adapter.run()

    assert len(RECEIVED) == NUM_MESSAGES
    assert all(len(payload) == MESSAGE_SIZE_BYTES for payload in RECEIVED)
