import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

# 10 minutes
MAX_MESSAGE_LATENCY = 600


class DownstreamBranch(Enum):
    DELAYED = "delayed"
    RECENT = "recent"


@dataclass
class Message:
    value: Any
    timestamp: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "timestamp": self.timestamp,
        }


def unpack_kafka_message(msg: str) -> Message:
    d = json.loads(msg)
    return Message(
        value=d["value"],
        timestamp=d["timestamp"],
    )


def should_send_to_blq(msg: Message) -> DownstreamBranch:
    timestamp = msg.timestamp
    if timestamp < time.time() - MAX_MESSAGE_LATENCY:
        return DownstreamBranch.DELAYED
    else:
        return DownstreamBranch.RECENT


def json_dump_message(msg: Message) -> str:
    return json.dumps(msg.to_dict())
