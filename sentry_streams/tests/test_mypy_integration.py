"""
Test mypy integration for Rust functions

This test verifies that mypy can detect type mismatches in pipelines using Rust functions.
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def test_rust_extension():  # type: ignore[no-untyped-def]
    """Build the test Rust extension before running tests"""
    test_crate_dir = Path(__file__).parent / "rust_test_functions"

    # Build the extension
    result = subprocess.run(
        ["maturin", "develop"], cwd=test_crate_dir, capture_output=True, text=True
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to build test Rust extension: {result.stderr}")

    # Import and return the module
    try:
        import rust_test_functions  # type: ignore[import-not-found]
    except ImportError as e:
        pytest.fail(f"Failed to import test Rust extension: {e}")

    yield rust_test_functions

    # Try to uninstall the test extension (best effort)
    try:
        subprocess.run(
            ["uv", "pip", "uninstall", "rust-test-functions"],
            capture_output=True,
        )
    except Exception:
        pass


def test_mypy_detects_correct_pipeline_rust(test_rust_extension) -> None:  # type: ignore[no-untyped-def]
    """Test that mypy accepts a correctly typed pipeline"""

    # Create a test file with correct types
    correct_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapString, TestMessage

def create_correct_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser("parser", msg_type=TestMessage))              # bytes -> TestMessage
        .apply(Filter("filter", function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply(Map("transform", function=TestMapCorrect()))         # TestMessage -> String
        .apply(Map("length", function=TestMapString()))             # String -> u64
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(correct_code)
        temp_file = f.name

        result = subprocess.run(
            [sys.executable, "-m", "mypy", temp_file, "--show-error-codes", "--check-untyped-defs"],
            capture_output=True,
            text=True,
        )

        assert result.stdout == "Success: no issues found in 1 source file\n"
        assert not result.stderr
        assert result.returncode == 0


@pytest.mark.xfail(reason="Type checking not working for both Rust and Python, apparently")
def test_mypy_detects_type_mismatch_rust(test_rust_extension) -> None:  # type: ignore[no-untyped-def]
    """Test that mypy detects type mismatches in pipeline definitions"""

    wrong_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapWrongType, TestMessage

# expect failure: TestMapWrongType expects Message[bool] but gets Message[TestMessage]
def create_wrong_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser("parser", msg_type=TestMessage))              # bytes -> TestMessage
        .apply(Filter("filter", function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply(Map("wrong", function=TestMapWrongType()))           # Expects Message[bool], gets Message[TestMessage]!
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(wrong_code)
        temp_file = f.name

    try:
        result = subprocess.run(
            [sys.executable, "-m", "mypy", temp_file, "--show-error-codes", "--check-untyped-defs"],
            capture_output=True,
            text=True,
        )

        # Should detect type mismatch
        assert result.returncode > 0
        assert (
            'Argument "function" to "Map" has incompatible type "TestMapWrongType"' in result.stdout
        )

    finally:
        Path(temp_file).unlink()


@pytest.mark.xfail(reason="Type checking not working for both Rust and Python, apparently")
def test_mypy_detects_type_mismatch_python() -> None:  # type: ignore[no-untyped-def]
    """Test that mypy detects type mismatches in pipeline definitions with Python functions"""

    wrong_code = """
from sentry_streams.pipeline.pipeline import Filter, Map, Parser, streaming_source, StreamSink
from sentry_streams.pipeline.message import Message

class TestMessage:
    pass

def filter_correct(msg: Message[TestMessage]) -> bool:
    return True

def map_wrong_type(msg: Message[bool]) -> str:  # This expects bool but will get TestMessage
    return "test"

# expect failure: map_wrong_type expects Message[bool] but gets Message[TestMessage]
def create_wrong_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply(Parser("parser", msg_type=TestMessage))              # bytes -> TestMessage
        .apply(Filter("filter", function=filter_correct))           # TestMessage -> TestMessage
        .apply(Map("wrong", function=map_wrong_type))               # Expects Message[bool], gets Message[TestMessage]!
        .sink(StreamSink("output", "output-stream"))
    )
"""

    # Write to temp file and run mypy
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(wrong_code)
        temp_file = f.name

    try:
        result = subprocess.run(
            [sys.executable, "-m", "mypy", temp_file, "--show-error-codes", "--check-untyped-defs"],
            capture_output=True,
            text=True,
        )

        # Should detect type mismatch
        assert result.returncode > 0
        assert 'Argument "function" to "Map" has incompatible type' in result.stdout

    finally:
        Path(temp_file).unlink()
