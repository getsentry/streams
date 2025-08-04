"""Integration tests for Rust functions in streaming pipelines"""

import json
from typing import Any, cast

import pytest
from arroyo.backends.kafka import KafkaConsumer, KafkaProducer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Partition, Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.adapters.arroyo.adapter import ArroyoAdapter
from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import PipelineConfig, RuntimeTranslator
from sentry_streams.dummy.dummy_adapter import DummyAdapter
from sentry_streams.pipeline.message import PyMessage as Message
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Serializer,
    StreamSink,
    streaming_source,
)
from sentry_streams.runner import iterate_edges


def test_basic_rust_function_execution(rust_test_functions: Any) -> None:
    """Test that Rust functions execute correctly in a pipeline"""
    from rust_test_functions import TestFilterCorrect, TestMapCorrect

    # TestMessage in Rust corresponds to dicts with id/content in Python
    test_messages = [
        Message(payload=cast(Any, {"id": 1, "content": "Hello"}), headers=[], timestamp=0.0),
        Message(
            payload=cast(Any, {"id": 0, "content": "Should be filtered"}), headers=[], timestamp=0.0
        ),
        Message(payload=cast(Any, {"id": 2, "content": "World"}), headers=[], timestamp=0.0),
    ]

    rust_filter = TestFilterCorrect()
    rust_map = TestMapCorrect()

    filtered_messages = []
    for msg in test_messages:
        if rust_filter(msg):
            filtered_messages.append(msg)

    mapped_messages = []
    for msg in filtered_messages:
        mapped_messages.append(rust_map(msg))

    assert len(filtered_messages) == 2
    # Type ignore because the payload is actually a dict at runtime but typed differently
    assert filtered_messages[0].payload["id"] == 1
    assert filtered_messages[1].payload["id"] == 2

    assert mapped_messages[0] == "Processed: Hello"
    assert mapped_messages[1] == "Processed: World"


def test_rust_python_interoperability(rust_test_functions: Any) -> None:
    """Test that Rust and Python functions work together in pipelines"""
    from rust_test_functions import TestFilterCorrect, TestMapCorrect

    def python_uppercase(msg: Message[str]) -> str:
        return msg.payload.upper()

    def python_length(msg: Message[str]) -> int:
        return len(msg.payload)

    test_msg = Message(payload=cast(Any, {"id": 5, "content": "test"}), headers=[], timestamp=0.0)

    rust_filter = TestFilterCorrect()
    rust_map = TestMapCorrect()

    if rust_filter(test_msg):
        rust_output = rust_map(test_msg)
        py_msg = Message(payload=rust_output, headers=[], timestamp=0.0)
        uppercase_output = python_uppercase(py_msg)
        length_msg = Message(payload=uppercase_output, headers=[], timestamp=0.0)
        final_output = python_length(length_msg)

        assert rust_output == "Processed: test"
        assert uppercase_output == "PROCESSED: TEST"
        assert final_output == 15


def test_error_handling_in_rust_functions(rust_test_functions: Any) -> None:
    """Test error handling when Rust functions fail"""
    from rust_test_functions import TestMapWrongType

    wrong_type_map = TestMapWrongType()

    # Note: We're deliberately passing wrong type (dict instead of bool) to test error handling
    test_msg = Message(payload=cast(Any, {"id": 1, "content": "test"}), headers=[], timestamp=0.0)

    with pytest.raises(TypeError, match="'dict' object cannot be converted to 'PyBool'"):
        wrong_type_map(test_msg)  # Intentionally wrong type for error testing


def test_complex_data_serialization(rust_test_functions: Any) -> None:
    """Test that complex data structures survive Rust function roundtrips"""
    from rust_test_functions import TestMapCorrect

    complex_msg = {"id": 12345, "content": 'Test with special chars: 你好 🚀 \n\t"quotes"'}

    rust_map = TestMapCorrect()
    msg = Message(payload=cast(Any, complex_msg), headers=[], timestamp=123456789.0)

    result = rust_map(msg)

    assert result == 'Processed: Test with special chars: 你好 🚀 \n\t"quotes"'

    empty_msg = Message(payload=cast(Any, {"id": 1, "content": ""}), headers=[], timestamp=0.0)
    assert rust_map(empty_msg) == "Processed: "


def test_chained_rust_functions(rust_test_functions: Any) -> None:
    """Test multiple Rust functions chained together"""
    from rust_test_functions import (
        TestFilterCorrect,
        TestMapCorrect,
        TestMapString,
    )

    rust_filter = TestFilterCorrect()
    rust_map_to_string = TestMapCorrect()
    rust_map_to_length = TestMapString()

    test_messages = [
        Message(payload=cast(Any, {"id": 3, "content": "Hello World!"}), headers=[], timestamp=0.0),
        Message(payload=cast(Any, {"id": 0, "content": "Filtered"}), headers=[], timestamp=0.0),
        Message(payload=cast(Any, {"id": 10, "content": "Short"}), headers=[], timestamp=0.0),
    ]

    results = []
    for msg in test_messages:
        if rust_filter(msg):
            string_result = rust_map_to_string(msg)
            string_msg = Message(payload=string_result, headers=[], timestamp=0.0)
            length_result = rust_map_to_length(string_msg)
            results.append(length_result)

    assert len(results) == 2
    assert results[0] == len("Processed: Hello World!")
    assert results[1] == len("Processed: Short")


def test_rust_functions_in_pipeline_structure(rust_test_functions: Any) -> None:
    """Test that Rust functions work in actual pipeline infrastructure"""
    from rust_test_functions import TestFilterCorrect, TestMapCorrect

    # Create a pipeline using the same structure as the examples
    # The pipeline will handle type conversion from bytes to TestMessage
    pipeline = (
        streaming_source("input", stream_name="test-messages")
        .apply(Filter("rust_filter", function=cast(Any, TestFilterCorrect())))
        .apply(Map("rust_map", function=cast(Any, TestMapCorrect())))
        .apply(Serializer("serializer"))
        .sink(StreamSink("output", stream_name="processed-messages"))
    )

    # Use the dummy adapter to verify pipeline structure
    dummy_config: PipelineConfig = {}
    adapter: DummyAdapter[Any, Any] = load_adapter("dummy", dummy_config, None)  # type: ignore
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(adapter)

    # Execute pipeline structure creation
    iterate_edges(pipeline, translator)

    # Verify that the pipeline was built with Rust functions
    assert "input" in adapter.input_streams
    assert "rust_filter" in adapter.input_streams
    assert "rust_map" in adapter.input_streams
    assert "output" in adapter.input_streams

    # Verify the pipeline structure includes our Rust function steps
    expected_streams = ["input", "rust_filter", "rust_map", "serializer", "output"]
    for stream in expected_streams:
        assert stream in adapter.input_streams, f"Missing stream: {stream}"


def test_rust_functions_with_message_flow(rust_test_functions: Any) -> None:
    """Test that Rust functions process actual messages through a pipeline"""
    from rust_test_functions import TestFilterCorrect, TestMapCorrect

    # This test demonstrates that Rust functions work in the pipeline infrastructure
    # by creating a pipeline and showing that messages flow through Rust functions
    # Create in-memory broker
    storage = MemoryMessageStorage[KafkaPayload]()
    broker = LocalBroker(storage, MockedClock())
    broker.create_topic(Topic("ingest-metrics"), 1)
    broker.create_topic(Topic("transformed-events"), 1)

    # Create pipeline that uses Rust functions
    def parse_json_bytes(msg: Message[bytes]) -> Any:
        """Parse JSON bytes and cast to dict for testing"""
        parsed_dict = json.loads(msg.payload.decode("utf-8"))
        return parsed_dict

    # Track processed messages to verify Rust functions executed
    processed_messages = []

    def capture_result(msg: Message[str]) -> str:
        """Capture the result from Rust map function"""
        processed_messages.append(msg.payload)
        return msg.payload

    pipeline = (
        streaming_source("input", stream_name="ingest-metrics")
        .apply(Map("json_parser", function=cast(Any, parse_json_bytes)))
        .apply(Filter("rust_filter", function=cast(Any, TestFilterCorrect())))
        .apply(Map("rust_map", function=cast(Any, TestMapCorrect())))
        .apply(Map("capture", function=cast(Any, capture_result)))
        .apply(Serializer("serializer"))
        .sink(StreamSink("output", stream_name="transformed-events"))
    )

    # Setup ArroyoAdapter with LocalBroker
    adapter = ArroyoAdapter.build(
        {
            "env": {},
            "steps_config": {
                "input": {"input": {}},
                "output": {"output": {}},
            },
        },
        {"input": cast(KafkaConsumer, broker.get_consumer("ingest-metrics"))},
        {"output": cast(KafkaProducer, broker.get_producer())},
    )

    # Configure and create pipeline processors
    iterate_edges(pipeline, RuntimeTranslator(adapter))
    adapter.create_processors()
    processor = adapter.get_processor("input")

    # Send test messages
    test_messages = [
        {"id": 1, "content": "Hello"},  # Should pass filter (id > 0)
        {"id": 0, "content": "Filtered"},  # Should be filtered out (id = 0)
        {"id": 2, "content": "World"},  # Should pass filter (id > 0)
    ]

    for msg in test_messages:
        broker.produce(
            Partition(Topic("ingest-metrics"), 0),
            KafkaPayload(None, json.dumps(msg).encode("utf-8"), []),
        )

    # Process messages through pipeline
    processor._run_once()  # Process first message
    processor._run_once()  # Process second message
    processor._run_once()  # Process third message

    # Verify that Rust functions processed the messages correctly
    # This demonstrates that Rust functions execute within pipeline infrastructure
    assert len(processed_messages) == 2  # Only messages with id > 0 passed the filter
    assert processed_messages[0] == "Processed: Hello"
    assert processed_messages[1] == "Processed: World"

    # The key success: Rust functions executed and transformed data within the pipeline!
