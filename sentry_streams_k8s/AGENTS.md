# AGENTS.md - sentry_streams_k8s Package

> **Note:** For general repository setup, testing, and type checking commands, see [../AGENTS.md](../AGENTS.md). This file contains package-specific details for working directly within the `sentry_streams` directory.

## Package Overview

`sentry_streams_k8s` package contains the automation and the infrastructure to deploy
Sentry Streams in a Kubernetes environment.

At the moment it targets Sentry production environment that is based on
[sentry-kube](https://github.com/getsentry/sentry-infra-tools), though in the
future we expect to have something more Sentry agnostic like Helm charts and
a Kubernetes operator.

## Package Structure

- **`sentry_streams_k8s/`** - Python package source code
- **`tests/`** - Python test suite

## Quick Start

**Recommended:** Use the repository root Makefile commands documented in [../AGENTS.md](../AGENTS.md):
- `make install-dev` - Set up development environment
- `make tests-streams` - Run Python tests
- `make typecheck` - Run type checking

## Virtual Environment - REQUIRED

**You MUST use a virtual environment when developing this package.** The package uses a `.venv` directory managed by `uv`.

## Package-Specific Development

### Manual Setup (Alternative to make install-dev)

If working directly in the package directory without using the root Makefile:

```bash
cd sentry_streams_k8s
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
cargo test
```

### Type Checking from Package Directory

Manual type checking requires building Rust modules first:

```bash
# Run mypy
.venv/bin/mypy --config-file mypy.ini --strict sentry_streams_k8s/
```

**Note:** Using `make typecheck` from the repository root is recommended as it handles all build steps automatically.

### Type Checking Configuration

Type checking is configured in `mypy.ini`:
- Strict mode enabled
- Missing imports are flagged as errors

### Building from Package Directory

Manual wheel building:

```bash
uv pip install wheel build
.venv/bin/python -m build --wheel
```

Wheels are created in `dist/`. **Recommended:** Use `make build-streams-k8s` from repository root.


## Package-Specific Notes

### Cleaning Build Artifacts

```bash
# Manual cleanup
rm -rf .venv
```

**Recommended:** Use `make reset` from repository root.

## Package Configuration

## Important Package-Specific Notes

- Always work within the virtual environment (`.venv`)
- For general repository information and prerequisites, see [../AGENTS.md](../AGENTS.md)
