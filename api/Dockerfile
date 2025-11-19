# --- Stage 1: Builder ---
FROM python:3.12-slim-bookworm AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/code/.venv

WORKDIR /code

# 1. Install Git (for versioning) and Build Tools (for compiling hnswlib/extensions)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git build-essential && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy Metadata Files
# We MUST include README.md because pyproject.toml references it
COPY .git /code/.git
COPY pyproject.toml uv.lock* README.md LICENSE /code/
COPY src /code/src

# 3. Create venv and install Core Library
RUN uv venv && uv pip install .

# 4. Install API Dependencies
COPY api/pyproject.toml /code/api/
WORKDIR /code/api
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install -r requirements.txt

# --- Stage 2: Runner ---
FROM python:3.12-slim-bookworm

WORKDIR /code

# 5. Copy the fully built virtual environment from the builder
COPY --from=builder /code/.venv /code/.venv

# Set path to use the venv automatically
ENV PATH="/code/.venv/bin:$PATH"
ENV PYTHONPATH=/code
ENV ATYPES_PATH=/code/api/predefined_types

# 6. Copy ONLY the API code
COPY api /code/api

EXPOSE 8000

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]