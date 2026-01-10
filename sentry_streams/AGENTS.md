# AGENTS.md - sentry_streams Package

> **Note:** For general repository setup, testing, and type checking commands, see [../AGENTS.md](../AGENTS.md). This file contains package-specific details for working directly within the `sentry_streams` directory.

## Package Overview

`sentry_streams` is the core streaming platform implementation for Sentry. It's a hybrid Rust/Python package built using Maturin that provides the fundamental APIs and runtime for building streaming applications.

It contains the Streaming application data model, the DSL to represent dataflow applications,
the runner CLI script and the Arroyo adapter to run the application on single nodes.

## Package Structure

- **`sentry_streams/`** - Python package source code
  - `adapters/` - Adapter implementations for different streaming backends
  - `pipeline/` - Pipeline construction and execution logic.
      This package contains the DSL to compose streaming applications,
      it contains the definition of the primitives and the parser.
  - `examples/` - Example streaming applications
  - `metrics/` - Metrics and monitoring utilities
- **`src/`** - Rust source code for the Rust runtime. It contains an Arroyo
      based consumer that is capable of running the pipeline defined above.
- **`tests/`** - Python test suite
- **`integration_tests/`** - Integration tests
- **`docs/`** - Sphinx documentation

## Quick Start

**Recommended:** Use the repository root Makefile commands documented in [../AGENTS.md](../AGENTS.md):
- `make install-dev` - Set up development environment
- `make tests-streams` - Run Python tests
- `make tests-integration` - Run integration tests
- `make tests-rust-streams` - Run Rust tests
- `make typecheck` - Run type checking
- `make build-streams` - Build wheel package
- `make docs` - Build documentation

## Virtual Environment - REQUIRED

**You MUST use a virtual environment when developing this package.** The package uses a `.venv` directory managed by `uv`.

## Package-Specific Development

### Manual Setup (Alternative to make install-dev)

If working directly in the package directory without using the root Makefile:

```bash
cd sentry_streams
uv sync
```

This creates `.venv/` and installs all dependencies including development tools.

### Running Tests from Package Directory

When not using the root Makefile:

```bash
# Python tests
.venv/bin/pytest -vv tests/

# Integration tests
.venv/bin/pytest -vv integration_tests/

# Specific test
.venv/bin/pytest -vv tests/test_pipeline.py::test_specific_function

# Rust tests
source .venv/bin/activate
source ../scripts/rust-envvars
cargo test
```

### Type Checking from Package Directory

Manual type checking requires building Rust modules first:

```bash
# Build Rust test modules first (required for complete type checking)
cd sentry_streams/examples/rust_simple_map_filter/rust_transforms/
source ../../../../.venv/bin/activate
maturin develop
cd -

cd tests/rust_test_functions/
source ../../.venv/bin/activate
maturin develop
cd -

# Run mypy
.venv/bin/mypy --config-file mypy.ini --strict sentry_streams/
```

**Note:** Using `make typecheck` from the repository root is recommended as it handles all build steps automatically.

### Type Checking Configuration

Type checking is configured in `mypy.ini`:
- Python version: 3.11
- Strict mode enabled
- Missing imports are flagged as errors
- Untyped imports are allowed for Rust modules (`rust_streams.*`)

### Building from Package Directory

Manual wheel building:

```bash
uv pip install wheel build
.venv/bin/python -m build --wheel
```

Wheels are created in `dist/`. **Recommended:** Use `make build-streams` from repository root.

### Building Documentation from Package Directory

```bash
uv sync --group docs
.venv/bin/sphinx-build -b html docs/source/ docs/build/
```

Documentation is built in `docs/build/`. **Recommended:** Use `make docs` from repository root.

## Package-Specific Notes

### Developing Rust Components

When making changes to Rust code:

```bash
# Rebuild the Rust module
source .venv/bin/activate
source ../scripts/rust-envvars
maturin develop

# Run Rust tests
cargo test

# Run Python tests that use Rust components
pytest -vv tests/
```

### Cleaning Build Artifacts

```bash
# Manual cleanup
rm -rf .venv
cargo clean
```

**Recommended:** Use `make reset` from repository root.

## Package Configuration

### Build System

- **Build backend:** `maturin>=1.5.1` - Handles both Python and Rust compilation
- **Python requirement:** `>=3.11`
- **Key runtime deps:** `sentry-arroyo`, `pyyaml`, `jsonschema`, `sentry-kafka-schemas`, `polars`, `click`, `datadog`

## Important Package-Specific Notes

- Always work within the virtual environment (`.venv`)
- Rust components must be built before type checking can complete successfully
- The package is a hybrid Rust/Python package using `maturin` as its build backend
- For general repository information and prerequisites, see [../AGENTS.md](../AGENTS.md)
