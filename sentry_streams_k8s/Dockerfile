# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create non-root user and group
RUN set -ex; \
    groupadd -r streaming-operator --gid 1000; \
    useradd -r -m -g streaming-operator --uid 1000 streaming-operator

# Install minimal OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

# Install Python dependencies declared in pyproject.toml, including the
# operator extra (kopf and kubernetes).
COPY pyproject.toml pyproject.toml
RUN uv pip install --system -r pyproject.toml --extra operator

# Copy application source (owned by non-root user)
COPY --chown=streaming-operator:streaming-operator README.md README.md
COPY --chown=streaming-operator:streaming-operator sentry_streams_k8s/ sentry_streams_k8s/

# Install the package
RUN uv pip install --system --no-deps .

# Ensure workspace ownership
RUN chown -R streaming-operator:streaming-operator /app

# Switch to non-root user
USER streaming-operator

ENTRYPOINT ["streams-k8s-operator"]
