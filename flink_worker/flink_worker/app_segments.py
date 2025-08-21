"""
Application-specific segment implementations for the Flink Worker.

This module contains concrete implementations of ProcessingSegment and WindowSegment
that provide specific business logic for different use cases.
"""

import json
import time
from typing import Any, Mapping, Sequence

from flink_worker.flink_worker_pb2 import Message
from flink_worker.segment import Accumulator, ProcessingSegment
import logging
logger = logging.getLogger(__name__)


class BatchingSegment(ProcessingSegment):
    """
    A processing segment that batches messages together.

    This segment collects messages into batches based on either:
    - A maximum batch size
    - A maximum time window

    When either condition is met, it flushes the batch as a single message.
    """

    def __init__(self, batch_size: int, batch_time_sec: int):
        self.batch_size = batch_size
        self.batch = []
        self.batch_time = 0
        self.batch_time_sec = batch_time_sec

    def __deserialize(self, payload: bytes) -> Mapping[str, Any]:
        """Deserialize a message payload from bytes to a dictionary."""
        return json.loads(payload.decode("utf-8"))

    def _maybe_flush(self) -> Sequence[Message]:
        if len(self.batch) == 0:
            return []

        # Flush if batch is full or time window exceeded
        if len(self.batch) >= self.batch_size or time.time() - self.batch_time > self.batch_time_sec:
            ret_payload = json.dumps(self.batch).encode("utf-8")
            size = len(self.batch)
            ret_msg = Message()
            ret_msg.payload = ret_payload
            ret_msg.timestamp = int(time.time() * 1000)
            ret_msg.headers["batch_size"] = str(size)
            ret_msg.headers["type"] = "Batch"

            # Reset batch state
            self.batch = []
            self.batch_time = 0
            logger.info(f"Flushed batch of {size} messages")
            return [ret_msg]
        return []

    def process(self, message: Message) -> Sequence[Message]:
        # Check if current batch should be flushed before adding new message
        flushed_messages = self._maybe_flush()

        # If this is the first message or batch was just flushed, set the batch time
        if self.batch_time == 0:
            self.batch_time = time.time()

        # Add the new message to the batch
        self.batch.append(self.__deserialize(message.payload))

        return flushed_messages

    def watermark(self, timestamp: int) -> Sequence[Message]:
        return self._maybe_flush()


class AppendEntry(ProcessingSegment):
    def process(self, message: Message) -> Sequence[Message]:
        # Deserialize the JSON payload
        try:
            payload_dict = json.loads(message.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # If payload is not valid JSON, create a new dict with the original payload as a string
            payload_dict = {"original_payload": message.payload.decode("utf-8", errors="replace")}

        # Add the key-value pair to the deserialized JSON
        payload_dict["processed"] = True

        # Create a new message with the modified payload
        new_message = Message()
        new_message.payload = json.dumps(payload_dict).encode("utf-8")

        # Copy headers if they exist
        if message.headers:
            for key, value in message.headers.items():
                new_message.headers[key] = value

        # Copy timestamp if it exists
        if message.timestamp:
            new_message.timestamp = message.timestamp

        return [new_message]

    def watermark(self, timestamp: int) -> Sequence[Message]:
        """Process a watermark event - no processing needed for this segment."""
        return []


class EventCounter(Accumulator):
    def __init__(self):
        self.count = 0

    def add(self, message: Message) -> None:
        self.count += 1

    def get_value(self) -> Message:
        return Message(payload=str(self.count).encode("utf-8"))
