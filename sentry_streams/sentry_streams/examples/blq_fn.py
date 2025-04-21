import time
from enum import Enum

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.message import Message

# 10 minutes
MAX_MESSAGE_LATENCY = 600


class DownstreamBranch(Enum):
    DELAYED = "delayed"
    RECENT = "recent"


def should_send_to_blq(msg: Message[IngestMetric]) -> DownstreamBranch:
    timestamp = msg.payload["timestamp"]  # We can do this because the type of the payload is known
    if timestamp < time.time() - MAX_MESSAGE_LATENCY:
        return DownstreamBranch.DELAYED
    else:
        return DownstreamBranch.RECENT
