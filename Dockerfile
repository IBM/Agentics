# Stage 1: Builder
FROM python:3.12-slim AS builder

# STRATEGY 1: Combine RUN commands.
# Every RUN instruction creates a new layer. By combining them, we create
# one dirty layer that we can clean up immediately in the same step.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install system dependencies
# We use --no-install-recommends to avoid installing useless extras (like docs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

# Patch versioning
RUN sed -i 's/dynamic = \["version"\]/version = "0.1.0"/' pyproject.toml && \
    sed -i '/uv-dynamic-versioning/d' pyproject.toml

# Generate lockfile
RUN uv lock

# Clean the uv cache after syncing.
# The --frozen flag installs from the lockfile.
# We immediately run `uv cache clean` to delete the downloaded wheels
# (which are duplicated in the venv anyway).
RUN uv sync --frozen --no-dev --no-install-project \
    && uv cache clean

# Stage 2: Runtime
FROM python:3.12-slim

# Create user first
RUN useradd -m appuser
WORKDIR /app

# Copy ONLY what is needed
# The builder stage's cache, apt packages, and gcc are all left behind.
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY src/agentics/api/applications/text2sql/data /app/data/text2sql

# Runtime Setup
RUN mkdir -p /app/temp_files && chown -R appuser:appuser /app

ENV PYTHONPATH="/app/src"
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

USER appuser
EXPOSE 8000

CMD ["uvicorn", "agentics.api.main:app", "--host", "0.0.0.0", "--port", "8000"]