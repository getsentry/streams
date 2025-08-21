"""
Tests for importing flink_worker modules
"""

import pytest


def test_flink_worker_segment_import() -> None:
    """Test that we can import the segment module from flink_worker."""
    try:
        from flink_worker.segment import (
            Accumulator,
            AccumulatorWindowSegment,
            ProcessingSegment,
            WindowSegment,
        )

        # Verify the classes are imported correctly
        assert ProcessingSegment is not None
        assert Accumulator is not None
        assert WindowSegment is not None
        assert AccumulatorWindowSegment is not None

        # Verify they are abstract base classes (ProcessingSegment and Accumulator should be)
        assert hasattr(ProcessingSegment, "__abstractmethods__")
        assert hasattr(Accumulator, "__abstractmethods__")

        print("Successfully imported all segment classes from flink_worker")

    except ImportError as e:
        pytest.fail(f"Failed to import flink_worker.segment: {e}")


def test_flink_worker_message_import() -> None:
    """Test that we can import the Message class from flink_worker."""
    try:
        from flink_worker.flink_worker_pb2 import Message

        # Verify the Message class is imported correctly
        assert Message is not None

        print("Successfully imported Message class from flink_worker")

    except ImportError as e:
        pytest.fail(f"Failed to import Message from flink_worker: {e}")


def test_flink_worker_service_import() -> None:
    """Test that we can import the service module from flink_worker."""
    try:
        from flink_worker.service import FlinkWorkerService

        # Verify the FlinkWorkerService class is imported correctly
        assert FlinkWorkerService is not None

        print("Successfully imported FlinkWorkerService from flink_worker")

    except ImportError as e:
        pytest.fail(f"Failed to import FlinkWorkerService from flink_worker: {e}")


def test_flink_worker_client_import() -> None:
    """Test that we can import the client module from flink_worker."""
    try:
        from flink_worker.client import FlinkWorkerClient

        # Verify the FlinkWorkerClient class is imported correctly
        assert FlinkWorkerClient is not None

        print("Successfully imported FlinkWorkerClient from flink_worker")

    except ImportError as e:
        pytest.fail(f"Failed to import FlinkWorkerClient from flink_worker: {e}")
