"""Shared test fixtures for sentry_streams tests"""

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(scope="module")
def rust_test_functions() -> Any:
    """Build and import the test Rust functions

    This fixture builds the rust_test_functions crate using maturin
    and makes it available for import in tests. The build happens
    once per test module for efficiency.

    Returns:
        module: The imported rust_test_functions module
    """
    test_crate_dir = Path(__file__).parent / "rust_test_functions"
    maturin_path = Path(sys.exec_prefix) / "bin/maturin"

    # Build the extension
    result = subprocess.run(
        [maturin_path, "develop"], cwd=test_crate_dir, capture_output=True, text=True
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to build test Rust extension: {result.stderr}")

    # Import and return the module
    try:
        import rust_test_functions
    except ImportError as e:
        pytest.fail(f"Failed to import test Rust extension: {e}")

    yield rust_test_functions

    # Clean up - try to uninstall the test extension (best effort)
    try:
        subprocess.run(
            ["uv", "pip", "uninstall", "rust-test-functions", "-y"],
            capture_output=True,
        )
    except Exception:
        pass
