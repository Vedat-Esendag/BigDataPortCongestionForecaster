"""FastAPI app. Run with: uvicorn api.main:app

Where each answer comes from:
    /ships, /ships/{mmsi}, /outlook   Redis (latest state)          clients/redis_client.py
    /history/ships, /history/outlook  Cassandra (history)           clients/cassandra_client.py
    /ws                               Redis keyspace events (push)  live.py
    /ports                            config/ports.yaml

The app keeps no data of its own, so any number of copies can run behind one Service.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from api.clients.cassandra_client import CassandraClient
from api.clients.redis_client import LiveStore, create_redis
from api.errors import StoreUnavailable
from api.live import ConnectionManager, LiveFeed
from api.ports import load_ports
from api.routes import health, history, outlook, ports, ships, ws
from api.settings import Settings, load_settings

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # On start: begin listening for Redis changes, and connect to Cassandra in the
        # background so the first history request doesn't pay for it. Neither blocks startup,
        # so the API comes up (and answers /health) even while Redis or Cassandra are down.
        feed = asyncio.create_task(LiveFeed(app.state.live, app.state.connections).run())
        warmup = asyncio.create_task(asyncio.to_thread(app.state.history.try_connect))
        yield
        # On stop: end the listener, then close the connections.
        feed.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await feed
        await warmup
        await app.state.live.redis.aclose()
        app.state.history.close()

    app = FastAPI(title="Port congestion API", version="0.2.0", lifespan=lifespan)
    # State is set here (not in lifespan) so the objects exist for tests that skip lifespan,
    # and so tests can swap in fakes before the app starts.
    app.state.settings = settings
    app.state.ports = load_ports(settings.ports_config)
    app.state.live = LiveStore(create_redis(settings), settings.ship_max_age)
    app.state.history = CassandraClient(settings)
    app.state.connections = ConnectionManager()

    @app.exception_handler(StoreUnavailable)
    async def store_unavailable(request: Request, error: StoreUnavailable) -> JSONResponse:
        # Logged without a stack trace: a store being down is an expected situation.
        log.warning("%s %s: %s", request.method, request.url.path, error)
        return JSONResponse(
            {"detail": f"{error.store} is unavailable, try again shortly", "store": error.store},
            status_code=503,
        )

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
