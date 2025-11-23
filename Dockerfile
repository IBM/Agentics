# Stage 1: Builder
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install compilers for hnswlib
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy configuration
COPY pyproject.toml ./

# We replace 'dynamic = ["version"]' with a static version
# and remove the dynamic-versioning build requirement.
RUN sed -i 's/dynamic = \["version"\]/version = "0.1.0"/' pyproject.toml && \
    sed -i '/uv-dynamic-versioning/d' pyproject.toml

# Generate lockfile for Linux (CPU Torch)
RUN uv lock

# Install dependencies
RUN uv sync --frozen --no-dev --no-install-project

RUN rm -rf /app/.venv/lib/python3.12/site-packages/nvidia*

# Stage 2: Runtime
FROM python:3.12-slim

RUN useradd -m appuser
WORKDIR /app

# 1. Copy Virtual Environment
COPY --from=builder /app/.venv /app/.venv

# 2. Copy Application Code
COPY src ./src

# 3. Copy "Baked-in" Data (The Source of Truth)
# We copy specifically to a clean data directory, separate from code
COPY src/agentics/api/applications/text2sql/data /app/data/text2sql

# 4. Create Temp Directory for Uploads (Ephemeral)
# Note: In prod, use S3, but this is needed for the app to accept the stream
RUN mkdir -p /app/temp_files && chown -R appuser:appuser /app

# Environment Setup
ENV PYTHONPATH="/app/src"
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Switch user
USER appuser
EXPOSE 8000

CMD ["uvicorn", "agentics.api.main:app", "--host", "0.0.0.0", "--port", "8000"]