"""FastAPI app. Run with: uvicorn api.main:app"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from api.clients.cassandra_client import CassandraClient
from api.clients.redis_client import create_redis
from api.live import ConnectionManager
from api.ports import load_ports
from api.routes import health, history, outlook, ports, ships, ws
from api.settings import Settings, load_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await app.state.redis.aclose()
        app.state.cassandra.close()

    app = FastAPI(title="Port congestion API", version="0.1.0", lifespan=lifespan)
    # State is set here (not in lifespan) so the objects exist for tests that skip lifespan.
    app.state.settings = settings
    app.state.ports = load_ports(settings.ports_config)
    app.state.redis = create_redis(settings)
    app.state.cassandra = CassandraClient(settings)
    app.state.connections = ConnectionManager()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_origins),
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    for router_module in (health, ports, ships, outlook, history, ws):
        app.include_router(router_module.router)
    app.mount("/metrics", make_asgi_app())
    return app


app = create_app()
