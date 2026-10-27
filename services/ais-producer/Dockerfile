# Build from the repo root (the image also needs config/ and schemas/):
#   docker build -f services/ais-producer/Dockerfile .
ARG PYTHON_VERSION=3.12
ARG UV_VERSION=0.13.6

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY services/ais-producer/pyproject.toml services/ais-producer/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY services/ais-producer/src ./src
RUN uv sync --frozen --no-dev

FROM python:${PYTHON_VERSION}-slim
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY config /app/config
COPY schemas/avro /app/schemas/avro
ENV PATH="/app/.venv/bin:$PATH" \
    PORTS_CONFIG=/app/config/ports.yaml \
    AVRO_SCHEMA_DIR=/app/schemas/avro \
    PYTHONUNBUFFERED=1
EXPOSE 9100
USER nobody
CMD ["python", "-m", "ais_producer"]
