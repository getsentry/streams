#!/usr/bin/env python3
"""
Example demonstrating how to use the in-memory testing functionality
for the Rust streaming pipeline.

This example shows how to:
1. Create test data
2. Configure a pipeline for in-memory testing
3. Run the pipeline with test data
4. Verify the results

Note: This requires the Rust extension to be compiled with the new in-memory features.
"""

import json
from typing import Any

from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.pipeline.chain import Map, StreamSink, streaming_source
from sentry_streams.pipeline.message import Message


def transform_message(msg: Message[bytes]) -> bytes:
    """Simple transformation function that adds a suffix to the message."""
    original = msg.payload.decode('utf-8')
    transformed = f"{original}_transformed"
    return transformed.encode('utf-8')


def main():
    """Main example function."""
    
    # Create test data
    test_messages = [
        b"hello world",
        b"test message 1", 
        b"test message 2",
        json.dumps({"key": "value", "number": 42}).encode('utf-8'),
    ]
    
    print(f"Test data: {[msg.decode('utf-8') for msg in test_messages]}")
    
    # Create a simple pipeline
    pipeline = (
        streaming_source("test_source", stream_name="test-input")
        .apply("transform", Map(transform_message))
        .sink("test_sink", StreamSink(stream_name="test-output"))
    )
    
    # Configure for in-memory testing
    config = {
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
    
    # Create adapter in test mode with test data
    adapter = RustArroyoAdapter.build(
        config,
        test_mode=True,
        test_data=test_messages
    )
    
    print("Built adapter in test mode")
    
    # Build the pipeline
    from sentry_streams.adapters.stream_adapter import RuntimeTranslator
    from sentry_streams.runner import iterate_edges
    
    iterate_edges(pipeline, RuntimeTranslator(adapter))
    
    print("Pipeline configured, running in test mode...")
    
    # Run the pipeline - this will process the test data in-memory
    try:
        adapter.run()
        print("Pipeline execution completed successfully!")
    except Exception as e:
        print(f"Error during pipeline execution: {e}")


if __name__ == "__main__":
    main()