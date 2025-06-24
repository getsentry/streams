FROM python:3.11.13-slim-bookworm AS base

WORKDIR /app

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


# Copy the entire project
COPY . .

RUN uv pip install --system --group=dev
RUN maturin build --release -o dist
RUN uv pip install --system -e .

# Install the built wheel
# RUN uv pip install target/wheels/*.whl

# Development stage with dev dependencies including py-spy
# FROM base AS dev

# Install dev dependencies including py-spy
#RUN uv sync --frozen --group dev

# Make the bin directory scripts executable
RUN chmod +x bin/pyspy_runner bin/runner

# Set the entry point to the pyspy_runner script
ENTRYPOINT ["bin/runner"]
