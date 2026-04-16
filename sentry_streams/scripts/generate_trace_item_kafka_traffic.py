#!/usr/bin/env python3
"""
Multi-process Kafka load generator producing sentry_protos TraceItem protobuf payloads.

Requires a normal sentry_streams dev install (confluent-kafka via sentry-arroyo,
sentry-protos via sentry-kafka-schemas).

Example::

    uv run python scripts/generate_trace_item_kafka_traffic.py \\
        --bootstrap-servers localhost:9092 \\
        --topic trace-items \\
        --workers 4 \\
        --messages-per-worker 500
"""

from __future__ import annotations

import random
import sys
import time
from datetime import datetime, timezone
from multiprocessing import Process
from typing import Final

import click
from confluent_kafka import KafkaError, Message, Producer
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1 import trace_item_pb2
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

TARGET_PAYLOAD_BYTES: Final[int] = 2560
SIZE_TOLERANCE: Final[int] = 120

# librdkafka local queue: cap matches max outstanding; throttle earlier so callbacks drain.
MAX_OUTSTANDING_MESSAGES: Final[int] = 100_000
HIGH_WATER_OUTSTANDING: Final[int] = 90_000


def _utc_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))
    return ts


def _build_trace_item(
    *,
    worker_id: int,
    message_index: int,
    rng: random.Random,
    header_item_type: int,
    blob_pad_len: int,
) -> trace_item_pb2.TraceItem:
    assert header_item_type in (1, 2)
    proto_type = (
        TraceItemType.TRACE_ITEM_TYPE_SPAN
        if header_item_type == 1
        else TraceItemType.TRACE_ITEM_TYPE_ERROR
    )
    item = trace_item_pb2.TraceItem()
    item.organization_id = 1 + (message_index % 11)
    item.project_id = 10_000 + worker_id * 97 + (message_index % 53)
    item.trace_id = rng.randbytes(16).hex()
    item.item_id = rng.randbytes(16)
    item.item_type = proto_type
    item.client_sample_rate = rng.random()
    item.server_sample_rate = min(1.0, rng.random() * 1.2)
    item.retention_days = 30 + (message_index % 90)
    item.timestamp.CopyFrom(_utc_timestamp())
    item.received.CopyFrom(_utc_timestamp())

    item.attributes["worker"].int_value = worker_id
    item.attributes["seq"].int_value = message_index
    item.attributes["region"].string_value = rng.choice(["us", "eu", "apac"])
    item.attributes["tier"].string_value = rng.choice(["free", "business", "enterprise"])
    item.attributes["release"].string_value = (
        f"streams-loadtest@{rng.randint(1, 99)}.{rng.randint(0, 20)}"
    )
    item.attributes["duration_ms"].double_value = rng.lognormvariate(3.0, 1.0)

    blob_seed = rng.randbytes(24).hex()
    prefix = f"w{worker_id}:n{message_index}:{blob_seed}:"
    filler_len = max(0, blob_pad_len - len(prefix))
    item.attributes["streams.loadtest.blob"].string_value = prefix + ("x" * filler_len)

    return item


def _serialize_near_target_size(
    *,
    worker_id: int,
    message_index: int,
    rng: random.Random,
    header_item_type: int,
) -> bytes:
    pad = 2100
    for _ in range(48):
        item = _build_trace_item(
            worker_id=worker_id,
            message_index=message_index,
            rng=rng,
            header_item_type=header_item_type,
            blob_pad_len=pad,
        )
        raw = item.SerializeToString()
        n = len(raw)
        if TARGET_PAYLOAD_BYTES - SIZE_TOLERANCE <= n <= TARGET_PAYLOAD_BYTES + SIZE_TOLERANCE:
            return raw
        if n < TARGET_PAYLOAD_BYTES:
            pad += max(8, (TARGET_PAYLOAD_BYTES - n) * 3 // 4)
        else:
            pad -= max(8, (n - TARGET_PAYLOAD_BYTES) * 3 // 4)
        pad = max(256, pad)

    item = _build_trace_item(
        worker_id=worker_id,
        message_index=message_index,
        rng=rng,
        header_item_type=header_item_type,
        blob_pad_len=pad,
    )
    return item.SerializeToString()


def _producer_worker(
    *,
    bootstrap_servers: str,
    topic: str,
    worker_id: int,
    messages_per_worker: int,
    base_seed: int | None,
) -> None:
    if base_seed is not None:
        rng: random.Random = random.Random(base_seed + worker_id * 1_000_003)
    else:
        rng = random.Random()

    outstanding = 0
    delivery_failures: list[KafkaError] = []

    def delivery_callback(err: KafkaError | None, _msg: Message) -> None:
        nonlocal outstanding
        outstanding -= 1
        if err is not None:
            delivery_failures.append(err)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "queue.buffering.max.messages": MAX_OUTSTANDING_MESSAGES,
        }
    )
    count = 0
    for i in range(messages_per_worker):
        if count % 1000 == 0:
            print(f"Produced {count} messages")
            count += 1

        header_item_type = 1 if rng.random() < 0.95 else 2
        payload = _serialize_near_target_size(
            worker_id=worker_id,
            message_index=i,
            rng=rng,
            header_item_type=header_item_type,
        )
        while True:
            while outstanding >= HIGH_WATER_OUTSTANDING:
                producer.poll(0.5)
            try:
                producer.produce(
                    topic,
                    value=payload,
                    # headers=[("item_type", str(header_item_type).encode("ascii"))],
                    callback=delivery_callback,
                )
                outstanding += 1
                producer.poll(0)
                break
            except BufferError:
                producer.poll(0.5)

    producer.flush()
    if delivery_failures:
        raise RuntimeError(
            f"worker {worker_id}: {len(delivery_failures)} message(s) failed delivery "
            f"(first: {delivery_failures[0]!r})"
        )


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--bootstrap-servers",
    "-b",
    required=True,
    help="Kafka bootstrap servers (comma-separated host:port list).",
)
@click.option("--topic", "-t", required=True, help="Topic to produce to.")
@click.option(
    "--workers",
    "-w",
    default=4,
    show_default=True,
    type=int,
    help="Number of producer processes.",
)
@click.option(
    "--messages-per-worker",
    "-n",
    default=100,
    show_default=True,
    type=int,
    help="Messages each worker produces.",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Optional RNG seed (each worker uses a deterministic derivative).",
)
def main(
    bootstrap_servers: str,
    topic: str,
    workers: int,
    messages_per_worker: int,
    seed: int | None,
) -> None:
    if workers < 1:
        raise click.BadParameter("workers must be at least 1")
    if messages_per_worker < 1:
        raise click.BadParameter("messages-per-worker must be at least 1")

    started = time.perf_counter()
    processes: list[Process] = []
    for wid in range(workers):
        p = Process(
            target=_producer_worker,
            kwargs={
                "bootstrap_servers": bootstrap_servers,
                "topic": topic,
                "worker_id": wid,
                "messages_per_worker": messages_per_worker,
                "base_seed": seed,
            },
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
        if p.exitcode not in (0, None):
            click.echo(f"Worker exited with code {p.exitcode}", err=True)
            sys.exit(p.exitcode or 1)

    elapsed = time.perf_counter() - started
    total = workers * messages_per_worker
    click.echo(
        f"Produced {total} messages from {workers} worker(s) to {topic!r} in {elapsed:.2f}s "
        f"({total / elapsed:.1f} msg/s)."
    )


if __name__ == "__main__":
    main()
