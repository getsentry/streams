FROM python:3.11.13-slim-bookworm AS build_base

WORKDIR /usr/src/sentry_streams

# Install system dependencies required for building Rust code and uv
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    pkg-config \
    patchelf \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN pip install uv

RUN uv pip install --system maturin>=1.5.1

FROM build_base AS base
# Install dependencies first as they change less often than the application
# so we should invalidate the cache less often
COPY ./pyproject.toml ./
RUN uv pip install --system -r pyproject.toml

FROM base AS rust_application

# We only copy sentry_streams as we do not need the flink part here
COPY . .

# TODO: Break the rust build into dependencies and application so
# we can cache the dependencies and only rebuild the application
# when dependencies do not change.
RUN maturin build --release -o dist --compatibility linux

FROM rust_application AS application

RUN set -ex; \
    groupadd -r streams --gid 1000; \
    useradd -r -g streams --uid 1000 streams; \
    chown -R streams:streams ./;

# RUN uv pip install --system target/wheels/*.whl
RUN uv pip install --system -e .

# Make the bin directory scripts executable
RUN chmod +x bin/runner
RUN chmod +x bin/pyspy_runner
RUN chmod +x ./docker_entrypoint.sh

USER streams
# Set the entry point to our custom script
ENTRYPOINT ["./docker_entrypoint.sh"]
