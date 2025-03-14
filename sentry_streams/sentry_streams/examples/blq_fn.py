import json
from dataclasses import dataclass
from datetime import datetime, timedelta
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

    @classmethod
    def unpack(cls, msg: str) -> "KafkaMessage":
        d = json.loads(msg)
        return cls(
            key=d["key"],
            headers=d["headers"],
            value=d["value"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )


def should_send_to_blq(msg: KafkaMessage) -> DownstreamBranch:
    timestamp = msg.timestamp
    if timestamp < datetime.now() - timedelta(minutes=10):
        return DownstreamBranch.DELAYED
    else:
        return DownstreamBranch.RECENT
