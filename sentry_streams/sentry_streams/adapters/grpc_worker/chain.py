import pickle
import time
from abc import ABC
from typing import Any, Callable, Generic, MutableSequence, Optional, Sequence, TypeVar

from flink_worker.flink_worker_pb2 import Message
from flink_worker.segment import ProcessingSegment

from sentry_streams.pipeline.message import Message as StreamsMessage
from sentry_streams.pipeline.message import PyMessage

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


class ChainStep(ABC, Generic[TIn, TOut]):
    """
    These wrap a step in the pipeline.
    The difference between this and ProcessingSegment is that it is defined
    in sentry_Streams and it deals with the StreamsMessage type which are
    not dependendent on the GRPC Wiorker.
    """

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[TOut]]:
        raise NotImplementedError

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        raise NotImplementedError


def to_streams_message(message: Message, schema: Optional[str]) -> Sequence[StreamsMessage[bytes]]:
    headers = [(key, value.encode()) for key, value in message.headers.items()]

    # CHeck if object is pickled
    try:
        payload = pickle.loads(message.payload)
    except Exception:
        payload = message.payload

    return PyMessage(
        payload,
        headers,
        float(message.timestamp) / 1000,
        schema,
    )


def to_message(message: StreamsMessage[bytes]) -> Message:
    return Message(
        payload=message.payload,
        headers={key: value.decode() for key, value in message.headers},
        timestamp=int(message.timestamp * 1000),
    )


class ChainSegment(ProcessingSegment):
    """A simple segment that passes through messages without modification."""

    # TODO: See if we can make these types meaningful.
    # These steps have to be chained by putting them in a List.
    # As each step may have a different types we cannot simply make this
    # class generic with respect to the input and output type.
    # We would not be able to define the type of the List.
    def __init__(
        self, steps: MutableSequence[ChainStep[Any, Any]], input_schema: Optional[str] = None
    ) -> None:
        super().__init__()
        self.steps = steps
        self.input_schema = input_schema

    def process(self, message: Message) -> Sequence[Message]:
        """Process the input data by returning the message as-is."""
        buffer = [to_streams_message(message, self.input_schema)]
        for step in self.steps:
            new_buffer = []
            for msg in buffer:
                new_buffer.extend(step.process(msg))
            buffer = new_buffer

        # Convert all messages to Message objects
        return [to_message(msg) for msg in buffer]

    def watermark(self, timestamp: int) -> Sequence[Message]:
        """Process a watermark event by calling each step's watermark method."""
        buffer = []
        for step in self.steps:
            # Add any messages from the step's watermark
            new_buffer = []
            for message in buffer:
                new_buffer.extend(step.process(message))
            new_buffer.extend(step.watermark(timestamp))
            buffer = new_buffer

        # Handle both single message and sequence cases
        return [to_message(msg) for msg in buffer]


class MapChainStep(ChainStep[TIn, TOut]):
    """A step that maps a message to a new message."""

    def __init__(self, func: Callable[[StreamsMessage[TIn]], TOut]) -> None:
        self.func = func

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[TOut]]:
        return [
            PyMessage(
                payload=self.func(message),
                headers=message.headers,
                timestamp=message.timestamp,
                schema=message.schema,
            )
        ]

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        return []


class FilterChainStep(ChainStep[TIn, TOut]):
    """A step that filters a message."""

    def __init__(self, func: Callable[[StreamsMessage[TIn]], bool]) -> None:
        self.func = func

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[TOut]]:
        return [message] if self.func(message) else []

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        return []


class FlatMapChainStep(ChainStep[TIn, TOut]):
    """A step that flat maps a message to a new message."""

    def __init__(self, func: Callable[[StreamsMessage[TIn]], Sequence[TOut]]) -> None:
        self.func = func

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[TOut]]:
        return [
            PyMessage(
                payload=payload,
                headers=message.headers,
                timestamp=message.timestamp,
                schema=message.schema,
            )
            for payload in self.func(message)
        ]

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        return []


class PickleStep(ChainStep[TIn, bytes]):
    """A step that pickles a message."""

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[bytes]]:
        return [
            PyMessage(
                payload=pickle.dumps(message.payload),
                headers=message.headers,
                timestamp=message.timestamp,
                schema=message.schema,
            )
        ]

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        return []


class UnpickleStep(ChainStep[bytes, TOut]):
    """A step that unpickles a message."""

    def process(self, message: StreamsMessage[bytes]) -> Sequence[StreamsMessage[TOut]]:
        return [
            PyMessage(
                payload=pickle.loads(message.payload),
                headers=message.headers,
                timestamp=message.timestamp,
                schema=message.schema,
            )
        ]
    
    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        return []


class BatchChainStep(ChainStep[TIn, TOut]):
    """
    A chain step that batches messages together.

    This step collects messages into batches based on either:
    - A maximum batch size
    - A maximum time window

    When either condition is met, it flushes the batch as a single message
    containing a sequence of payloads.
    """

    def __init__(self, batch_size: int, batch_time_sec: int):
        self.batch_size = batch_size
        self.batch_time_sec = batch_time_sec
        self.batch: list[StreamsMessage[TIn]] = []
        self.batch_start_time: float = 0.0

    def _should_flush(self) -> bool:
        """Check if the batch should be flushed based on size or time."""
        current_time = time.time()
        return len(self.batch) >= self.batch_size or (
            self.batch_start_time > 0
            and current_time - self.batch_start_time >= self.batch_time_sec
        )

    def _flush_batch(self) -> Sequence[StreamsMessage[TOut]]:
        """Flush the current batch and return it as a single message."""
        if not self.batch:
            return []

        # Create a sequence of payloads from the batch
        batch_payloads: Sequence[TIn] = [msg.payload for msg in self.batch]

        # Create a new message with the batch payloads
        # Use the timestamp of the first message in the batch
        batch_timestamp = self.batch[0].timestamp

        # Create headers indicating this is a batch
        batch_headers = [("batch_size", str(len(self.batch)).encode()), ("type", b"Batch")]

        # Create the batch message
        batch_message = PyMessage[Sequence[TIn]](
            payload=batch_payloads,
            headers=batch_headers,
            timestamp=batch_timestamp,
            schema=self.batch[0].schema,
        )

        # Reset batch state
        self.batch = []
        self.batch_start_time = 0.0

        return [batch_message]

    def process(self, message: StreamsMessage[TIn]) -> Sequence[StreamsMessage[TOut]]:
        # Check if current batch should be flushed before adding new message
        flushed_messages = []
        if self._should_flush():
            flushed_messages = self._flush_batch()

        # If this is the first message or batch was just flushed, set the batch start time
        if not self.batch:
            self.batch_start_time = time.time()

        # Add the new message to the batch
        self.batch.append(message)

        return flushed_messages

    def watermark(self, timestamp: int) -> Sequence[StreamsMessage[TOut]]:
        """Process a watermark event by flushing the batch if time constraint is met."""
        if self._should_flush():
            return self._flush_batch()
        return []
