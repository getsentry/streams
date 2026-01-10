# AGENTS.md - Streams Repository

## Repository Overview

This repository contains the Sentry Streaming Platform.
The Streaming Platform provides the infrastructure and the runtime to build and run
Kafka based streaming stateful and stateless streaming applications.

## Repository Structure

The repository is organized into two main Python packages:

- **`sentry_streams/`** - The core streaming platform implementation (hybrid Rust/Python package)
  - See [sentry_streams/AGENTS.md](./sentry_streams/AGENTS.md) for package-specific development instructions

- **`sentry_flink/`** - Apache Flink adapter for the streaming platform (pure Python package)
  - See [sentry_flink/AGENTS.md](./sentry_flink/AGENTS.md) for package-specific development instructions

### Other Directories

- **`platforms/`** - Docker and deployment configurations
- **`scripts/`** - Utility scripts for development and deployment

## Makefile Commands

The root Makefile provides commands for common development tasks across both packages:

### Setup and Installation

```bash
make install-dev
```
Installs development dependencies for both packages using `uv`. This will:
- Download required Flink JARs
- Install `uv` package manager if not present
- Set up virtual environments for both `sentry_streams` and `sentry_flink`

```bash
make install-pre-commit-hook
```
Installs pre-commit hooks for code quality checks.

```bash
make reset
```
Removes all build artifacts and virtual environments for a clean slate.

### Testing

```bash
make tests-streams
```
Runs Python tests for the `sentry_streams` package.

```bash
make tests-integration
```
Runs integration tests for `sentry_streams`.

```bash
make tests-rust-streams
```
Runs Rust tests for the `sentry_streams` package.

```bash
make tests-flink
```
Runs tests for the `sentry_flink` package.

### Type Checking

```bash
make typecheck
```
Runs `mypy` type checking on both packages. This will:
- Build Rust modules needed for type checking
- Run strict type checking on `sentry_streams`
- Run strict type checking on `sentry_flink`

### Building and Documentation

```bash
make build-streams
```
Builds the `sentry_streams` wheel package.

```bash
make docs
```
Builds the Sphinx documentation for the streaming platform.

## Development Prerequisites

- **Python 3.11+** (Note: `sentry_flink` requires <3.12 due to PyFlink limitations)
- **Rust toolchain** (for `sentry_streams` development)
- **uv** package manager (automatically installed by `make install-dev`)
- **direnv** (recommended for automatic environment setup)

## Working with Individual Packages

For detailed development instructions specific to each package, including virtual environment requirements, testing, and type checking, see the AGENTS.md files in each package directory:

- [sentry_streams/AGENTS.md](./sentry_streams/AGENTS.md)
- [sentry_flink/AGENTS.md](./sentry_flink/AGENTS.md)

## Additional Notes

- The repository uses `direnv` for automatic environment setup. Run `direnv allow` when prompted.
- If encountering CMake errors, the `.envrc` should set `CMAKE_POLICY_VERSION_MINIMUM=3.5` automatically.
- For GCP interactions, run `gcloud auth application-default login`.
- Documentation is available at: https://getsentry.github.io/streams/
