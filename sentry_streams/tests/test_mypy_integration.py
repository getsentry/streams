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


def test_mypy_detects_correct_pipeline(test_rust_extension) -> None:  # type: ignore[no-untyped-def]
    """Test that mypy accepts a correctly typed pipeline"""

    # Create a test file with correct types
    correct_code = """
from sentry_streams.pipeline import Filter, Map, Parser, streaming_source
from sentry_streams.pipeline.chain import StreamSink
from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapString, TestMessage

def create_correct_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply("parser", Parser(msg_type=TestMessage))              # bytes -> TestMessage
        .apply("filter", Filter(function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply("transform", Map(function=TestMapCorrect()))         # TestMessage -> String
        .apply("length", Map(function=TestMapString()))             # String -> u64
        .sink("output", StreamSink("output-stream"))
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


def test_mypy_detects_type_mismatch(test_rust_extension) -> None:  # type: ignore[no-untyped-def]
    """Test that mypy detects type mismatches in pipeline definitions"""

    wrong_code = """
from sentry_streams.pipeline import Filter, Map, Parser, streaming_source
from sentry_streams.pipeline.chain import StreamSink
from rust_test_functions import TestFilterCorrect, TestMapCorrect, TestMapWrongType, TestMessage

# expect failure: TestMapWrongType expects Message[bool] but gets Message[TestMessage]
def create_wrong_pipeline():
    return (
        streaming_source("input", "test-stream")
        .apply("parser", Parser(msg_type=TestMessage))              # bytes -> TestMessage
        .apply("filter", Filter(function=TestFilterCorrect()))      # TestMessage -> TestMessage
        .apply("wrong", Map(function=TestMapWrongType()))           # Expects Message[bool], gets Message[TestMessage]!
        .sink("output", StreamSink("output-stream"))
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


def test_rust_functions_have_proper_types(test_rust_extension) -> None:  # type: ignore[no-untyped-def]
    """Test that Rust functions expose proper type information"""

    filter_func = test_rust_extension.TestFilterCorrect()
    map_func = test_rust_extension.TestMapCorrect()
    wrong_func = test_rust_extension.TestMapWrongType()

    # Check that they have the required methods for type checking
    assert hasattr(filter_func, "__call__")
    assert hasattr(map_func, "__call__")
    assert hasattr(wrong_func, "__call__")

    # Check that they're callable
    assert callable(filter_func)
    assert callable(map_func)
    assert callable(wrong_func)

    # Check protocol compliance
    from sentry_streams.pipeline.rust_function_protocol import (
        RustFilterFunction,
        RustMapFunction,
    )

    assert isinstance(filter_func, RustFilterFunction)
    assert isinstance(map_func, RustMapFunction)
    assert isinstance(wrong_func, RustMapFunction)
