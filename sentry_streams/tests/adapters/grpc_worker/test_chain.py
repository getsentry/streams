"""
Tests for the ChainSegment class
"""

import json
from unittest.mock import patch

from flink_worker.flink_worker_pb2 import Message

from sentry_streams.adapters.grpc_worker.chain import (
    BatchChainStep,
    ChainSegment,
    FilterChainStep,
    MapChainStep,
    to_message,
    to_streams_message,
)
from sentry_streams.pipeline.message import PyMessage


def test_chain_segment_creation():
    """Test that ChainSegment can be instantiated."""
    segment = ChainSegment([])
    assert isinstance(segment, ChainSegment)


def test_chain_segment_process_message():
    """Test that ChainSegment.process returns the input message unchanged."""
    segment = ChainSegment([])

    # Create a test message
    message = Message(
        payload=b"test payload", headers={"key1": "value1", "key2": "value2"}, timestamp=123456789
    )

    # Process the message
    result = segment.process(message)

    # Should return a sequence with one message
    assert isinstance(result, list)
    assert len(result) == 1

    # The returned message should be the same as the input
    returned_message = result[0]
    assert returned_message.payload == message.payload
    assert returned_message.timestamp == message.timestamp

    # Check headers
    assert len(returned_message.headers) == len(message.headers)
    for key, value in message.headers.items():
        assert returned_message.headers[key] == value


def test_chain_segment_watermark():
    """Test that ChainSegment.watermark returns an empty sequence."""
    segment = ChainSegment([])

    # Test with different timestamps
    result1 = segment.watermark(123456789)
    result2 = segment.watermark(987654321)

    # Both should return empty sequences
    assert result1 == []
    assert result2 == []
    assert isinstance(result1, list)
    assert isinstance(result2, list)


def test_to_streams_message():
    """Test that to_streams_message converts Message to StreamsMessage correctly."""
    # Create a test Message
    message = Message(
        payload=b"test payload", headers={"key1": "value1", "key2": "value2"}, timestamp=123456789
    )

    # Convert to streams message
    result = to_streams_message(message, "test_schema")

    # Check the result
    assert isinstance(result, PyMessage)
    assert result.payload == b"test payload"
    assert result.timestamp == 123456.789  # timestamp divided by 1000
    assert result.schema == "test_schema"

    # Check headers are properly structured as (str, bytes) tuples
    assert len(result.headers) == 2
    # Convert headers to dict for easier testing
    header_dict = dict(result.headers)
    assert header_dict["key1"] == b"value1"
    assert header_dict["key2"] == b"value2"


def test_to_message():
    """Test that to_message converts StreamsMessage to Message correctly."""
    # Create a test StreamsMessage
    streams_message = PyMessage(
        payload=b"test payload",
        headers=[("key1", b"value1"), ("key2", b"value2")],
        timestamp=123.456,
        schema="test_schema",
    )

    # Convert to Message
    result = to_message(streams_message)

    # Check the result
    assert isinstance(result, Message)
    assert result.payload == b"test payload"
    assert result.timestamp == 123456  # timestamp multiplied by 1000
    assert len(result.headers) == 2
    assert result.headers["key1"] == "value1"
    assert result.headers["key2"] == "value2"


@patch("time.time")
def test_batch_chain_step_creation(mock_time):
    """Test that BatchChainStep can be instantiated with correct parameters."""
    mock_time.return_value = 100.0
    step = BatchChainStep(batch_size=10, batch_time_sec=5)
    assert step.batch_size == 10
    assert step.batch_time_sec == 5
    assert step.batch == []
    assert step.batch_start_time == 0.0


@patch("time.time")
def test_batch_chain_step_watermark_empty_batch(mock_time):
    """Test that watermark on an empty batch returns empty sequence."""
    mock_time.return_value = 100.0
    step = BatchChainStep(batch_size=5, batch_time_sec=10)

    # Call watermark on empty batch
    result = step.watermark(99)

    # Should return empty sequence
    assert result == []
    assert isinstance(result, list)


@patch("time.time")
def test_batch_chain_step_watermark_before_conditions_met(mock_time):
    """Test that watermark before batch size or time are attained returns empty sequence."""
    mock_time.side_effect = [100.0, 100.0, 101.0, 101.0]  # Extra values to avoid StopIteration
    step = BatchChainStep(batch_size=5, batch_time_sec=10)

    # Add one message
    message = PyMessage(
        payload="test message", headers=[("key", b"value")], timestamp=123.456, schema="test_schema"
    )

    # Process the message
    result = step.process(message)

    # Should return empty sequence (no flush)
    assert result == []

    # Call watermark immediately after (only 1 second passed, less than 10 sec limit)
    watermark_result = step.watermark(102)

    # Should return empty sequence (time not exceeded)
    assert watermark_result == []


@patch("time.time")
def test_batch_chain_step_batch_size_overflow(mock_time):
    """Test that batch is flushed when batch size is exceeded."""
    # Need more values: _should_flush calls + batch_start_time calls for each message
    mock_time.side_effect = [
        100.0,
        100.0,
        100.1,
        100.1,
        100.2,
        100.2,
        100.3,
        100.3,
        100.4,
        100.4,
        100.5,
        100.5,
    ]
    step = BatchChainStep(batch_size=4, batch_time_sec=100)

    # Add 5 messages (exceeding batch size of 4)
    for i in range(4):
        message = PyMessage(
            payload=f"message {i}",
            headers=[("key", b"value")],
            timestamp=123.456 + i,
            schema="test_schema",
        )
        result = step.process(message)
        assert result == []

    result = step.process(
        PyMessage(
            payload=f"message {i}",
            headers=[("key", b"value")],
            timestamp=128.456,
            schema="test_schema",
        )
    )
    assert len(result) == 1
    assert isinstance(result[0], PyMessage)

    # Check the batch message
    batch_message = result[0]
    assert isinstance(batch_message.payload, list)
    assert len(batch_message.payload) == 4


@patch("time.time")
def test_batch_chain_step_time_based_flush(mock_time):
    """Test that batch is flushed when time limit is exceeded."""
    # Mock time to return specific values
    # Need multiple calls: _should_flush + batch_start_time for each process, plus watermark calls
    mock_time.side_effect = [
        100.0,
        100.0,
        100.0,
        102.0,
        102.0,
        102.0,
        102.0,
    ]  # Extra values to avoid StopIteration

    step = BatchChainStep(batch_size=100, batch_time_sec=1)

    # Add first message at time 100.0
    message1 = PyMessage(
        payload="message 1", headers=[("key1", b"value1")], timestamp=123.456, schema="test_schema"
    )

    # Process first message
    result1 = step.process(message1)
    assert result1 == []  # No flush
    assert len(step.batch) == 1

    watermark_result = step.watermark(102)

    # Should flush due to time constraint
    assert len(watermark_result) == 1
    assert isinstance(watermark_result[0], PyMessage)

    # Check the batch message
    batch_message = watermark_result[0]
    assert isinstance(batch_message.payload, list)
    assert len(batch_message.payload) == 1


@patch("time.time")
def test_chain_segment_with_map_and_filter_steps(mock_time):
    """Test ChainSegment with MapChainStep and FilterChainStep."""
    mock_time.return_value = 100.0

    # Create a map step that doubles the payload
    def double_payload(message):
        return message.payload * 2

    # Create a filter step that only allows messages with payload > 5
    def filter_large_payload(message):
        return int(message.payload) > 5

    map_step = MapChainStep(double_payload)
    filter_step = FilterChainStep(filter_large_payload)

    # Create ChainSegment with both steps
    chain = ChainSegment([map_step, filter_step])

    # Test message that should pass the filter (payload 4 -> "44" after map, and 44 > 5)
    message1 = Message(payload=b"4", headers={"key": "value"}, timestamp=123456789)

    result1 = chain.process(message1)
    assert len(result1) == 1
    assert result1[0].payload == b"44"  # "4" doubled to "44"

    # Test message that should not pass the filter (payload 1 -> "11" after map, but 11 > 5, so let's use 0)
    message2 = Message(payload=b"0", headers={"key": "value"}, timestamp=123456789)

    result2 = chain.process(message2)
    assert len(result2) == 0  # Filtered out (0 -> "00" -> 0, and 0 <= 5)

    # Test watermark - should always return empty sequence
    watermark_result = chain.watermark(123456789)
    assert watermark_result == []


@patch("time.time")
def test_chain_segment_with_map_and_batch_steps(mock_time):
    """Test ChainSegment with MapChainStep followed by BatchChainStep."""
    # Mock time to return specific values for batch timing
    # Need values for: 3 process calls + 1 watermark call
    call_count = 0

    def mock_time_func():
        nonlocal call_count
        call_count += 1
        if call_count <= 4:
            return 100.0
        else:
            return 115.0  # 15 seconds later for watermark

    mock_time.side_effect = mock_time_func

    # Create a map step that converts payload to uppercase
    def parse_payload(message):
        return json.loads(message.payload.decode())

    def serialize_payload(message):
        return json.dumps(message.payload).encode()

    parse_step = MapChainStep(parse_payload)
    batch_step = BatchChainStep(batch_size=5, batch_time_sec=10)
    serialize_step = MapChainStep(serialize_payload)

    # Create ChainSegment with both steps
    chain = ChainSegment([parse_step, batch_step, serialize_step])

    # Add three messages without seeing results (batch size is 5, so no flush yet)
    messages = []
    for i in range(3):
        message = Message(
            payload=f'{{"value": {i}}}'.encode(),
            headers={"key": f"value{i}"},
            timestamp=123456789 + i,
        )
        result = chain.process(message)
        # Should return empty sequence since batch hasn't been flushed
        assert result == []
        messages.append(message)

    # Call watermark which should flush the batch due to time constraint
    # (15 seconds have passed, exceeding the 10 second limit)
    watermark_result = chain.watermark(123456789)

    # Should return one batched message
    assert len(watermark_result) == 1
    batch_message = watermark_result[0]

    # Check that the batch message is a Message object
    assert isinstance(batch_message, Message)

    # The payload should be the first message from the batch (due to serialization)
    assert batch_message.payload == b'[{"value": 0}, {"value": 1}, {"value": 2}]'

    # Check headers
    assert "batch_size" in batch_message.headers
    assert batch_message.headers["batch_size"] == "3"
    assert "type" in batch_message.headers
    assert batch_message.headers["type"] == "Batch"
