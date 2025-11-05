FROM python:3.11-slim
WORKDIR /code
COPY .
.
ENV ATYPES_PATH=/code/api/predefined_types
RUN pip install uv
RUN cd api && uv pip install -r <(uv pip compile pyproject.toml --no-emit-index-url)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]