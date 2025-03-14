import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum


class DownstreamBranch(Enum):
    DELAYED = "delayed"
    RECENT = "recent"


@dataclass
class KafkaMessage:
    key: str
    headers: dict[str, str]
    value: str
    timestamp: datetime


def unpack_kafka_message(msg: str) -> KafkaMessage:
    d = json.loads(msg)
    return KafkaMessage(
        key=d["key"],
        headers=d["headers"],
        value=d["value"],
        timestamp=datetime.fromisoformat(d["timestamp"]),
    )


def should_send_to_blq(msg: KafkaMessage) -> DownstreamBranch:
    timestamp = msg.timestamp
    if timestamp < datetime.now(timezone.utc) - timedelta(minutes=10):
        return DownstreamBranch.DELAYED
    else:
        return DownstreamBranch.RECENT
