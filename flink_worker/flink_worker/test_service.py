"""
Test script for the Flink Worker gRPC service.

This script tests the service methods directly without requiring a real gRPC server.
"""

import unittest
from unittest.mock import Mock
from typing import Sequence, MutableMapping

from flink_worker.flink_worker_pb2 import (
    Message, ProcessMessageRequest, ProcessMessageResponse, ProcessWatermarkRequest,
    AddToWindowRequest, TriggerWindowRequest, WindowIdentifier
)
from flink_worker.service import FlinkWorkerService
from flink_worker.segment import ProcessingSegment, WindowSegment
from google.protobuf import empty_pb2


class FakeProcessingSegment(ProcessingSegment):
    """Fake implementation of ProcessingSegment for testing."""

    def __init__(self, return_messages: Sequence[Message] = None):
        self.return_messages = return_messages or []
        self.processed_messages = []
        self.watermark_timestamps = []

    def process(self, message: Message) -> Sequence[Message]:
        """Process a message and return predefined messages."""
        self.processed_messages.append(message)
        return self.return_messages

    def watermark(self, timestamp: int) -> Sequence[Message]:
        """Process a watermark and return predefined messages."""
        self.watermark_timestamps.append(timestamp)
        return self.return_messages


class FakeWindowSegment(WindowSegment):
    """Fake implementation of WindowSegment for testing."""

    def __init__(self, return_messages: Sequence[Message] = None):
        self.return_messages = return_messages or []
        self.added_messages = []
        self.triggered_windows = []

    def add(self, message: Message, window_time: int, window_key: str) -> None:
        """Add a message to the window."""
        self.added_messages.append((message, window_time, window_key))

    def trigger(self, window_time: int, window_key: str) -> Sequence[Message]:
        """Trigger the window and return predefined messages."""
        self.triggered_windows.append((window_time, window_key))
        return self.return_messages


class TestFlinkWorkerService(unittest.TestCase):
    """Test cases for FlinkWorkerService."""

    def setUp(self):
        """Set up test fixtures."""
        # Create fake segments
        self.fake_processing_segment = FakeProcessingSegment()
        self.fake_window_segment = FakeWindowSegment()

        # Create test messages
        self.test_message = Message(
            payload=b"Hello, Flink Worker! This is a test message.",
            headers={
                "source": "test_script",
                "type": "greeting",
                "priority": "high"
            },
            timestamp=1234567890
        )

        self.test_response_message = Message(
            payload=b"Processed response message",
            headers={"status": "processed"},
            timestamp=1234567891
        )

        # Create segments mapping
        self.segments: MutableMapping[int, ProcessingSegment] = {
            42: self.fake_processing_segment
        }

        self.window_segments: MutableMapping[int, WindowSegment] = {
            42: self.fake_window_segment
        }

        # Create service instance
        self.service = FlinkWorkerService(self.segments, self.window_segments)

        # Create mock context
        self.mock_context = Mock()

    def test_process_message_success(self):
        """Test successful message processing."""
        # Configure fake segment to return messages
        self.fake_processing_segment.return_messages = [self.test_response_message]

        # Create request
        request = ProcessMessageRequest(
            message=self.test_message,
            segment_id=42
        )

        # Call service method
        response = self.service.ProcessMessage(request, self.mock_context)

        # Verify response
        self.assertIsInstance(response, ProcessMessageResponse)
        self.assertEqual(len(response.messages), 1)
        self.assertEqual(response.messages[0].payload, b"Processed response message")

        # Verify segment was called
        self.assertEqual(len(self.fake_processing_segment.processed_messages), 1)
        self.assertEqual(
            self.fake_processing_segment.processed_messages[0].payload,
            b"Hello, Flink Worker! This is a test message."
        )

    def test_process_message_invalid_segment(self):
        """Test message processing with invalid segment ID."""
        request = ProcessMessageRequest(
            message=self.test_message,
            segment_id=999  # Invalid segment ID
        )

        # Call service method
        response = self.service.ProcessMessage(request, self.mock_context)

        # Verify error was set on context
        self.mock_context.set_code.assert_called_once()
        self.mock_context.set_details.assert_called_once()

        # Verify response is empty
        self.assertEqual(len(response.messages), 0)

    def test_process_watermark_success(self):
        """Test successful watermark processing."""
        # Configure fake segment to return messages
        self.fake_processing_segment.return_messages = [self.test_response_message]

        # Create request
        request = ProcessWatermarkRequest(
            timestamp=1234567890,
            headers={"source": "test_script"},
            segment_id=42
        )

        # Call service method
        response = self.service.ProcessWatermark(request, self.mock_context)

        # Verify response
        self.assertIsInstance(response, ProcessMessageResponse)
        self.assertEqual(len(response.messages), 1)

        # Verify segment was called
        self.assertEqual(len(self.fake_processing_segment.watermark_timestamps), 1)
        self.assertEqual(self.fake_processing_segment.watermark_timestamps[0], 1234567890)

    def test_process_watermark_invalid_segment(self):
        """Test watermark processing with invalid segment ID."""
        request = ProcessWatermarkRequest(
            timestamp=1234567890,
            headers={"source": "test_script"},
            segment_id=999  # Invalid segment ID
        )

        # Call service method
        response = self.service.ProcessWatermark(request, self.mock_context)

        # Verify error was set on context
        self.mock_context.set_code.assert_called_once()
        self.mock_context.set_details.assert_called_once()

        # Verify response is empty
        self.assertEqual(len(response.messages), 0)

    def test_add_to_window_success(self):
        """Test successful addition to window."""
        # Create request
        window_id = WindowIdentifier(
            partition_key="test_partition",
            window_start_time=1234567890
        )
        request = AddToWindowRequest(
            message=self.test_message,
            segment_id=42,
            window_id=window_id
        )

        # Call service method
        response = self.service.AddToWindow(request, self.mock_context)

        # Verify response
        self.assertIsInstance(response, empty_pb2.Empty)

        # Verify segment was called
        self.assertEqual(len(self.fake_window_segment.added_messages), 1)
        message, window_time, window_key = self.fake_window_segment.added_messages[0]
        self.assertEqual(message.payload, b"Hello, Flink Worker! This is a test message.")
        self.assertEqual(window_time, 1234567890)
        self.assertEqual(window_key, "test_partition")

    def test_add_to_window_invalid_segment(self):
        """Test adding to window with invalid segment ID."""
        window_id = WindowIdentifier(
            partition_key="test_partition",
            window_start_time=1234567890
        )
        request = AddToWindowRequest(
            message=self.test_message,
            segment_id=999,  # Invalid segment ID
            window_id=window_id
        )

        # Call service method
        response = self.service.AddToWindow(request, self.mock_context)

        # Verify error was set on context
        self.mock_context.set_code.assert_called_once()
        self.mock_context.set_details.assert_called_once()

        # Verify response is still returned
        self.assertIsInstance(response, empty_pb2.Empty)

    def test_trigger_window_success(self):
        """Test successful window triggering."""
        # Configure fake segment to return messages
        self.fake_window_segment.return_messages = [self.test_response_message]

        # Create request
        window_id = WindowIdentifier(
            partition_key="test_partition",
            window_start_time=1234567890
        )
        request = TriggerWindowRequest(
            segment_id=42,
            window_id=window_id
        )

        # Call service method
        response = self.service.TriggerWindow(request, self.mock_context)

        # Verify response
        self.assertIsInstance(response, ProcessMessageResponse)
        self.assertEqual(len(response.messages), 1)

        # Verify segment was called
        self.assertEqual(len(self.fake_window_segment.triggered_windows), 1)
        window_time, window_key = self.fake_window_segment.triggered_windows[0]
        self.assertEqual(window_time, 1234567890)
        self.assertEqual(window_key, "test_partition")

    def test_trigger_window_invalid_segment(self):
        """Test window triggering with invalid segment ID."""
        window_id = WindowIdentifier(
            partition_key="test_partition",
            window_start_time=1234567890
        )
        request = TriggerWindowRequest(
            segment_id=999,  # Invalid segment ID
            window_id=window_id
        )

        # Call service method
        response = self.service.TriggerWindow(request, self.mock_context)

        # Verify error was set on context
        self.mock_context.set_code.assert_called_once()
        self.mock_context.set_details.assert_called_once()

        # Verify response is empty
        self.assertEqual(len(response.messages), 0)


def run_tests():
    """Run all tests and display results."""
    print("🧪 Running Flink Worker Service Tests...")
    print("=" * 50)

    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestFlinkWorkerService)

    # Run tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Display summary
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print(f"❌ {len(result.failures)} tests failed, {len(result.errors)} tests had errors")

    return result.wasSuccessful()


if __name__ == "__main__":
    run_tests()
