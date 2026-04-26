#!/usr/bin/env python3
"""
Build one TraceItem payload like ``generate_trace_item_kafka_traffic.py``,
configure log metrics, then run ``fake_transform`` many times on a PyRawMessage.

Example::

    uv run python scripts/benchmark_fake_transform_trace_item.py
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import random
from google.protobuf.timestamp_pb2 import Timestamp
import sys
import time
from pathlib import Path
from types import ModuleType

from sentry_protos.snuba.v1 import trace_item_pb2
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_streams.adapters.arroyo.steps_chain import fake_transform, input_metrics, output_metrics
from sentry_streams.metrics import DatadogMetricsConfig, LogMetricsConfig, configure_metrics
from sentry_streams.pipeline.message import PyRawMessage

import cProfile
import pstats

# Logical stream name for ``sentry_kafka_schemas.get_codec`` (see ``items_spans`` example).
TRACE_ITEM_STREAM_SCHEMA = "snuba-items"
DEFAULT_ITERATIONS = 100_000


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fake_transform repeatedly on one serialized TraceItem."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help="Number of fake_transform calls to run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.iterations < 1:
        raise ValueError("--iterations must be at least 1")

    item = _build_trace_item(
        worker_id=0,
        message_index=100,
        rng=random.Random(),
        header_item_type=1,
        blob_pad_len=2100,
    )
    payload = item.SerializeToString()

    log_config: LogMetricsConfig = {
        "type": "log",
        "period_sec": 120.0,
        "tags": {"script": "benchmark_fake_transform_trace_item"},
    }
    datadog_config: DatadogMetricsConfig = {
        "type": "datadog",
        "host": "datadog-agent",
        "port": 8125,
        "tags": {
            "sentry_region": "sandbox",
            "pipeline": "benchmark_fake_transform_trace_item",
        },
    }
    configure_metrics(datadog_config)

    msg = PyRawMessage(
        payload,
        [],
        time.time(),
        TRACE_ITEM_STREAM_SCHEMA,
    )

    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(args.iterations):
        # input_metrics("fake_step", len(msg.payload))
        # output_metrics("fake_step", None, time.time(), len(msg.payload))
        fake_transform(msg)
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    stats.print_stats(30)

    print(f"Completed {args.iterations} iterations of fake_transform.")


if __name__ == "__main__":
    main()
