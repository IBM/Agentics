# --- Stage 1: Builder ---
FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/code/.venv

WORKDIR /code

# 1. Install build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy Metadata
COPY pyproject.toml uv.lock* README.md LICENSE /code/
COPY src /code/src

# Patch pyproject.toml
RUN sed -i 's/dynamic = \["version"\]/version = "0.1.0"/' pyproject.toml

# 3. Create venv
RUN uv venv

# The "--mount=type=cache" flag stores downloaded wheels on host computer's disk,
# not inside the Docker image layer, preventing "No space left on device".

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install torch --index-url https://download.pytorch.org/whl/cpu

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install .

# 4. Install API Dependencies
COPY api/pyproject.toml /code/api/
WORKDIR /code/api

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install -r requirements.txt

# --- Stage 2: Runner ---
FROM python:3.12-slim-bookworm

WORKDIR /code

COPY --from=builder /code/.venv /code/.venv

ENV PATH="/code/.venv/bin:$PATH"
ENV PYTHONPATH=/code
ENV ATYPES_PATH=/code/api/predefined_types

COPY api /code/api

EXPOSE 8000

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]