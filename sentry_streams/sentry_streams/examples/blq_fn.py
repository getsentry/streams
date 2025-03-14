import json
import time
from dataclasses import dataclass
from enum import Enum

# 10 minutes
MAX_MESSAGE_DELAY = 600000


class DownstreamBranch(Enum):
    DELAYED = "delayed"
    RECENT = "recent"


@dataclass
class Message:
    value: str
    timestamp: float


def unpack_kafka_message(msg: str) -> Message:
    d = json.loads(msg)
    return Message(
        value=d["value"],
        timestamp=d["timestamp"],
    )


def should_send_to_blq(msg: Message) -> DownstreamBranch:
    timestamp = msg.timestamp
    if timestamp < time.time() - MAX_MESSAGE_DELAY:
        return DownstreamBranch.DELAYED
    else:
        return DownstreamBranch.RECENT
