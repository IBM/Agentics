FROM python:3.12-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/code \
    ATYPES_PATH=/code/api/predefined_types

WORKDIR /code

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV VIRTUAL_ENV=/code/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN uv venv

COPY pyproject.toml uv.lock* /code/
COPY src /code/src

RUN uv pip install .

COPY api/pyproject.toml /code/api/

WORKDIR /code/api
RUN uv pip compile pyproject.toml -o requirements.txt && uv pip install -r requirements.txt

COPY api /code/api

EXPOSE 8000

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]