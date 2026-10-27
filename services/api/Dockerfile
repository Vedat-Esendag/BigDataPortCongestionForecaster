# Build from the repo root (the image also needs config/):
#   docker build -f services/api/Dockerfile .
ARG PYTHON_VERSION=3.12
ARG UV_VERSION=0.13.6

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY services/api/pyproject.toml services/api/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY services/api/src ./src
RUN uv sync --frozen --no-dev

FROM python:${PYTHON_VERSION}-slim
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY config /app/config
ENV PATH="/app/.venv/bin:$PATH" \
    PORTS_CONFIG=/app/config/ports.yaml \
    PYTHONUNBUFFERED=1
EXPOSE 8000
USER nobody
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
