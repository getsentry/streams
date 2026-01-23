# AGENTS.md - sentry_flink Package

> **Note:** For general repository setup, testing, and type checking commands, see [../AGENTS.md](../AGENTS.md). This file contains package-specific details for working directly within the `sentry_flink` directory.

## Package Overview

`sentry_flink` is the Apache Flink adapter for the Sentry Streaming Platform. It provides integration between the Sentry Streams API and Apache Flink, enabling streaming applications to run on Flink's distributed execution engine.

## Package Structure

- **`sentry_flink/`** - Python package source code
  - `flink/` - Flink-specific implementations
  - `flink_runtime/` - Runtime utilities for Flink execution
- **`tests/`** - Test suite
  - `adapters/` - Adapter-specific tests
  - `flink_runtime/` - Runtime tests

## Quick Start

**Recommended:** Use the repository root Makefile commands documented in [../AGENTS.md](../AGENTS.md):
- `make install-dev` - Set up development environment
- `make tests-flink` - Run tests
- `make typecheck` - Run type checking

## Important: Python Version Constraint

⚠️ **PyFlink requires Python 3.11 (not 3.12+)** ⚠️

This package specifically requires Python `>=3.11,<3.12` due to PyFlink limitations. Ensure your Python version is 3.11.x before proceeding.

## Virtual Environment - REQUIRED

**You MUST use a virtual environment when developing this package.** The package uses a `.venv` directory managed by `uv`.

## Package-Specific Development

### Manual Setup (Alternative to make install-dev)

If working directly in the package directory without using the root Makefile:

```bash
cd sentry_flink
uv sync
```

This creates `.venv/` and installs all dependencies including Apache Flink and development tools.

### Running Tests from Package Directory

When not using the root Makefile:

```bash
# All tests
.venv/bin/pytest -vv tests/

# Specific test file
.venv/bin/pytest -vv tests/adapters/test_flink.py

# Specific test function
.venv/bin/pytest -vv tests/test_pipeline.py::test_specific_function
```

### Type Checking from Package Directory

```bash
.venv/bin/mypy --config-file mypy.ini --strict sentry_flink/
```

**Note:** Using `make typecheck` from the repository root is recommended as it checks all packages.

### Type Checking Configuration

Type checking is configured in `mypy.ini`:
- Python version: 3.11
- Strict mode enabled
- Missing imports are flagged as errors
- Untyped imports are allowed for PyFlink modules (`pyflink.*`)

## Running Flink Applications Locally

PyFlink applications can run with an embedded Flink cluster without requiring a standalone Flink server.

### Basic Local Execution

```bash
# Activate virtual environment
source .venv/bin/activate

# Run your Flink application
python your_flink_app.py
```

The application will start an embedded Flink cluster automatically.

## Package Configuration

### Development Dependencies

Key development dependencies (from `pyproject.toml`):
- `pytest` - Testing framework
- `mypy` - Type checking
- `pre-commit` - Code quality hooks
- `responses` - HTTP mocking for tests
- Type stubs: `types-requests`

### Runtime Dependencies

Key runtime dependencies:
- `apache-flink==2.1.0` - Apache Flink Python API
- `sentry-streams>=0.0.16` - Core streaming platform (installed automatically with `make install-dev`)
- `jsonschema` - JSON schema validation
- `requests` - HTTP client
- `setuptools==75.8.0` - Required by PyFlink (pinned version)

### Build System

- **Build backend:** `setuptools>=70`
- **Python requirement:** `>=3.11,<3.12` (PyFlink limitation)
