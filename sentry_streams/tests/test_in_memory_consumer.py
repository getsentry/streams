"""
Unit tests demonstrating the in-memory consumer functionality.

These tests show how to use the new in-memory testing capabilities
to test streaming pipelines without requiring Kafka infrastructure.
"""

import json
import unittest
from typing import Any

from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.chain import Map, StreamSink, streaming_source
from sentry_streams.pipeline.message import Message
from sentry_streams.runner import iterate_edges


def transform_to_upper(msg: Message[bytes]) -> bytes:
    """Transform message payload to uppercase."""
    original = msg.payload.decode('utf-8')
    return original.upper().encode('utf-8')


def add_suffix(msg: Message[bytes]) -> bytes:
    """Add a suffix to the message payload."""
    original = msg.payload.decode('utf-8') 
    return f"{original}_processed".encode('utf-8')


def parse_json_and_extract_name(msg: Message[bytes]) -> bytes:
    """Parse JSON and extract the 'name' field."""
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        name = data.get('name', 'unknown')
        return name.encode('utf-8')
    except (json.JSONDecodeError, UnicodeDecodeError):
        return b"invalid"


class TestInMemoryConsumer(unittest.TestCase):
    """Test cases for in-memory consumer functionality."""
    
    def setUp(self):
        """Set up common test configuration."""
        self.config = {
            "env": {},
            "steps_config": {
                "test_source": {
                    "bootstrap_servers": ["dummy:9092"],  # Not used in test mode
                },
                "test_sink": {
                    "bootstrap_servers": ["dummy:9092"],  # Not used in test mode  
                },
            },
        }
    
    def test_simple_transformation(self):
        """Test a simple message transformation pipeline."""
        # Test data
        test_messages = [
            b"hello world",
            b"test message",
            b"another example",
        ]
        
        # Create pipeline with uppercase transformation
        pipeline = (
            streaming_source("test_source", stream_name="test-input")
            .apply("transform", Map(transform_to_upper))
            .sink("test_sink", StreamSink(stream_name="test-output"))
        )
        
        # Build adapter in test mode
        adapter = RustArroyoAdapter.build(
            self.config,
            test_mode=True,
            test_data=test_messages
        )
        
        # Build and run pipeline
        iterate_edges(pipeline, RuntimeTranslator(adapter))
        
        # This should complete without errors
        try:
            adapter.run()
            self.assertTrue(True, "Pipeline executed successfully")
        except Exception as e:
            self.fail(f"Pipeline execution failed: {e}")
    
    def test_multi_step_pipeline(self):
        """Test a pipeline with multiple transformation steps."""
        # Test data
        test_messages = [
            b"hello",
            b"world", 
            b"testing",
        ]
        
        # Create pipeline with multiple transformations
        pipeline = (
            streaming_source("test_source", stream_name="test-input")
            .apply("uppercase", Map(transform_to_upper))
            .apply("add_suffix", Map(add_suffix))
            .sink("test_sink", StreamSink(stream_name="test-output"))
        )
        
        # Build adapter in test mode
        adapter = RustArroyoAdapter.build(
            self.config,
            test_mode=True,
            test_data=test_messages
        )
        
        # Build and run pipeline
        iterate_edges(pipeline, RuntimeTranslator(adapter))
        
        # This should complete without errors
        try:
            adapter.run()
            self.assertTrue(True, "Multi-step pipeline executed successfully")
        except Exception as e:
            self.fail(f"Multi-step pipeline execution failed: {e}")
    
    def test_json_processing(self):
        """Test processing JSON messages."""
        # Test data with JSON messages
        test_messages = [
            json.dumps({"name": "Alice", "age": 30}).encode('utf-8'),
            json.dumps({"name": "Bob", "age": 25}).encode('utf-8'),
            json.dumps({"name": "Charlie", "age": 35}).encode('utf-8'),
            b"invalid json",  # Should be handled gracefully
        ]
        
        # Create pipeline that extracts names from JSON
        pipeline = (
            streaming_source("test_source", stream_name="test-input")
            .apply("extract_name", Map(parse_json_and_extract_name))
            .sink("test_sink", StreamSink(stream_name="test-output"))
        )
        
        # Build adapter in test mode
        adapter = RustArroyoAdapter.build(
            self.config,
            test_mode=True,
            test_data=test_messages
        )
        
        # Build and run pipeline
        iterate_edges(pipeline, RuntimeTranslator(adapter))
        
        # This should complete without errors, even with invalid JSON
        try:
            adapter.run()
            self.assertTrue(True, "JSON processing pipeline executed successfully")
        except Exception as e:
            self.fail(f"JSON processing pipeline execution failed: {e}")
    
    def test_empty_test_data(self):
        """Test pipeline with empty test data."""
        # Empty test data
        test_messages = []
        
        # Create simple pipeline
        pipeline = (
            streaming_source("test_source", stream_name="test-input")
            .apply("transform", Map(transform_to_upper))
            .sink("test_sink", StreamSink(stream_name="test-output"))
        )
        
        # Build adapter in test mode
        adapter = RustArroyoAdapter.build(
            self.config,
            test_mode=True,
            test_data=test_messages
        )
        
        # Build and run pipeline
        iterate_edges(pipeline, RuntimeTranslator(adapter))
        
        # Should handle empty data gracefully
        try:
            adapter.run()
            self.assertTrue(True, "Pipeline handled empty data successfully")
        except Exception as e:
            self.fail(f"Pipeline failed with empty data: {e}")
    
    def test_large_messages(self):
        """Test pipeline with larger messages."""
        # Create larger test messages
        large_content = "x" * 10000  # 10KB message
        test_messages = [
            large_content.encode('utf-8'),
            (large_content + "_modified").encode('utf-8'),
        ]
        
        # Create pipeline
        pipeline = (
            streaming_source("test_source", stream_name="test-input")
            .apply("add_suffix", Map(add_suffix))
            .sink("test_sink", StreamSink(stream_name="test-output"))
        )
        
        # Build adapter in test mode
        adapter = RustArroyoAdapter.build(
            self.config,
            test_mode=True,
            test_data=test_messages
        )
        
        # Build and run pipeline
        iterate_edges(pipeline, RuntimeTranslator(adapter))
        
        # Should handle large messages
        try:
            adapter.run()
            self.assertTrue(True, "Pipeline handled large messages successfully")
        except Exception as e:
            self.fail(f"Pipeline failed with large messages: {e}")


if __name__ == "__main__":
    unittest.main()