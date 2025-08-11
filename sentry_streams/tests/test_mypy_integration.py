"""
Test mypy integration for type checking

This test verifies that mypy can detect type mismatches in pipelines.
"""

import subprocess
import sys
from pathlib import Path


def test_mypy_detects_correct_pipeline(tmp_path: Path) -> None:
    """Test that mypy accepts a correctly typed pipeline"""

    # Create a test file with correct types
    correct_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from sentry_streams.pipeline.message import Message
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric
from sentry_streams.examples.transform_metrics import transform_msg
from typing import Any, Mapping

def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")

def create_correct_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser[IngestMetric]("parser"))                      # bytes -> IngestMetric
        .apply(Filter("filter", function=filter_events))            # IngestMetric -> IngestMetric
        .apply(Map("transform", function=transform_msg))            # IngestMetric -> Mapping[str, Any]
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    test_file = tmp_path / "test_correct.py"
    test_file.write_text(correct_code)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            str(test_file),
            "--show-error-codes",
            "--check-untyped-defs",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Success: no issues found in 1 source file" in result.stdout


def test_mypy_detects_type_mismatch(tmp_path: Path) -> None:
    """Test that mypy detects type mismatches in pipeline definitions"""

    wrong_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from sentry_streams.pipeline.message import Message
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric
from sentry_streams.examples.transform_metrics import transform_msg
from typing import Any, Mapping

def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")

# expect failure: second transform_msg expects Message[IngestMetric] but gets Message[Mapping[str, Any]]
def create_wrong_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser[IngestMetric]("parser"))              # bytes -> IngestMetric
        .apply(Filter("filter", function=filter_events))    # IngestMetric -> IngestMetric
        .apply(Map("transform", function=transform_msg))    # IngestMetric -> Mapping[str, Any]
        .apply(Map("wrong", function=transform_msg))        # Expects Message[IngestMetric], gets Message[Mapping[str, Any]]!
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    test_file = tmp_path / "test_wrong.py"
    test_file.write_text(wrong_code)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            str(test_file),
            "--show-error-codes",
            "--check-untyped-defs",
        ],
        capture_output=True,
        text=True,
    )

    # Should detect type mismatch
    assert result.returncode > 0
    assert 'Argument "function" to "Map" has incompatible type' in result.stdout
    assert "expected" in result.stdout and "Mapping" in result.stdout


def test_mypy_detects_correct_pipeline_rust(tmp_path: Path, rust_test_functions: object) -> None:
    """Test that mypy accepts a correctly typed pipeline"""

    # Create a test file with correct types
    correct_code = """
from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapString, TestMessage
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from sentry_streams.pipeline.message import Message
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric
from sentry_streams.examples.transform_metrics import transform_msg
from typing import Any, Mapping

def filter_events(msg: Message[IngestMetric]) -> bool:
    return bool(msg.payload["type"] == "c")

def create_correct_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser[TestMessage]("parser"))                       # bytes -> TestMessage
        .apply(Filter("filter", function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply(Map("transform", function=TestMapCorrect()))         # TestMessage -> String
        .apply(Map("length", function=TestMapString()))             # String -> u64
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    test_file = tmp_path / "test_correct.py"
    test_file.write_text(correct_code)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            str(test_file),
            "--show-error-codes",
            "--check-untyped-defs",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Success: no issues found in 1 source file" in result.stdout


def test_mypy_detects_type_mismatch_rust(tmp_path: Path, rust_test_functions: object) -> None:
    """Test that mypy detects type mismatches in pipeline definitions"""

    wrong_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink

from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapWrongType, TestMessage

# expect failure: TestMapWrongType expects Message[bool] but gets Message[TestMessage]
def create_wrong_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser[TestMessage]("parser"))                       # bytes -> TestMessage
        .apply(Filter("filter", function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply(Map("wrong", function=TestMapWrongType()))           # Expects Message[bool], gets Message[TestMessage]!
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    test_file = tmp_path / "test_wrong.py"
    test_file.write_text(wrong_code)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            str(test_file),
            "--show-error-codes",
            "--check-untyped-defs",
        ],
        capture_output=True,
        text=True,
    )

    # Should detect type mismatch
    assert result.returncode > 0
    assert 'Argument "function" to "Map" has incompatible type' in result.stdout
    assert "expected" in result.stdout
    assert "TestMessage" in result.stdout
