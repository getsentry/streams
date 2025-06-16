import os
from typing import Any, Mapping

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.message import Message

num = 0


def transform_msg(msg: Message[IngestMetric]) -> Mapping[str, Any]:
    global num
    num += 1
    print(f"Current PID: {os.getpid()} {num}")
    return {**msg.payload, "transformed": True}
