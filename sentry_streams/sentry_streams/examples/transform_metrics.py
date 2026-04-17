import os
from typing import Any, Mapping

from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.message import Message

num = 0

CODEC = get_codec("snuba-items")


def transform_msg(msg: Message[IngestMetric]) -> Mapping[str, Any]:
    global num
    num += 1
    print(f"Current PID: {os.getpid()} {num}")
    return {**msg.payload, "transformed": True}


def transform_raw(msg: Message[bytes]) -> Mapping[str, Any]:
    CODEC.decode(msg.payload)
    return {"transformed": True}


def filter_events(msg: Message[IngestMetric]) -> bool:
    print(f"Filtering event: {msg.payload}")
    return bool(msg.payload["type"] == "c")


def fast_filter_events(message) -> bool:
    headers = message.headers
    for k, v in headers:
        if k == "item_type":
            return int(v) == 1
    return False
