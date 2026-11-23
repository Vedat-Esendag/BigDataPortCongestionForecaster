"""Live push (F10): Redis says a key changed, and the API sends the new value to the dashboards.

How a new prediction reaches the planner's screen:

    Spark ─► Kafka ship.prediction ─► Redis sink: JSON.SET ship.prediction:219000123
                                                  │
              Redis publishes "json.set" on __keyspace@0__:ship.prediction:219000123
                                                  │
    every API pod: LiveFeed receives it ─► JSON.GET that key ─► ConnectionManager.broadcast
                                                  │
                         each dashboard connected to this pod gets {"type": "ship", ...}

Why the detour through keyspace events: the Redis sink can store keys but cannot publish
messages (docs/connectors.md). Redis announces writes itself when notify-keyspace-events is on
(infra/k8s/base/redis/redis.conf).

Every API pod runs its own LiveFeed and pushes only to its own WebSocket clients. The pods share
nothing, so any number of them can run side by side, and any one can be killed.

Pub/sub doesn't store messages. While this pod is disconnected from Redis it misses the changes
made in that time. The dashboard loads the full state over REST when it (re)connects. That
covers a dashboard reconnecting, but not this pod's own short Redis outage: those clients keep
the missed values until the next update of each ship or slot.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket
from prometheus_client import Counter, Gauge

from api.clients.redis_client import (
    OUTLOOK_KEY_PREFIX,
    REDIS_DOWN,
    SHIP_KEY_PREFIX,
    LiveStore,
    parse_key,
)
from api.errors import StoreUnavailable, parse_documents
from api.models import OutlookSlot, Ship

log = logging.getLogger(__name__)

WS_CONNECTIONS = Gauge("api_websocket_connections", "Open dashboard WebSocket connections")
LIVE_FEED_UP = Gauge("api_live_feed_connected", "1 while this pod listens to Redis changes")
PUSHED = Counter("api_live_messages_total", "Changes pushed to dashboards, by kind", ["kind"])

# A client that doesn't take a message within this time is dropped. It reconnects by itself,
# and one stuck browser must not hold up everyone else's updates.
SEND_TIMEOUT_SECONDS = 5.0


class ConnectionManager:
    """The open WebSockets of this API pod, and which port each one wants updates for."""

    def __init__(self) -> None:
        # socket -> port id, or None for "every port" (what a client gets until it subscribes)
        self._sockets: dict[WebSocket, str | None] = {}

    async def connect(self, socket: WebSocket) -> None:
        await socket.accept()
        self._sockets[socket] = None
        WS_CONNECTIONS.set(len(self._sockets))

    def subscribe(self, socket: WebSocket, port: str | None) -> None:
        """Send this client only changes for `port` from now on (None: every port)."""
        if socket in self._sockets:
            self._sockets[socket] = port

    def disconnect(self, socket: WebSocket) -> None:
        self._sockets.pop(socket, None)
        WS_CONNECTIONS.set(len(self._sockets))

    async def broadcast(self, message: dict[str, Any], port: str | None = None) -> None:
        """Send one JSON message to every client that wants `port`, all at the same time.

        Clients that fail or are too slow are dropped. Their browser reconnects and reloads.
        """
        text = json.dumps(message, default=str)
        targets = [s for s, wanted in self._sockets.items() if wanted is None or wanted == port]
        results = await asyncio.gather(
            *(asyncio.wait_for(s.send_text(text), SEND_TIMEOUT_SECONDS) for s in targets),
            return_exceptions=True,
        )
        for socket, result in zip(targets, results, strict=True):
            if isinstance(result, BaseException):
                self.disconnect(socket)


class LiveFeed:
    """Background task: listens to Redis keyspace events and pushes each change.

    Started once per API pod when the app starts (main.py), stopped when it shuts down. If Redis
    goes away it waits and reconnects by itself (1 s, 2 s, 4 s ... up to 30 s between attempts).
    """

    def __init__(self, store: LiveStore, connections: ConnectionManager) -> None:
        self.store = store
        self.connections = connections
        self._was_connected = False
        db = store.redis.connection_pool.connection_kwargs.get("db", 0)
        # Keyspace events arrive on __keyspace@<db>__:<key>
        self.channel_prefix = f"__keyspace@{db}__:"
        self.patterns = [
            f"{self.channel_prefix}{SHIP_KEY_PREFIX}*",
            f"{self.channel_prefix}{OUTLOOK_KEY_PREFIX}*",
        ]

    async def run(self) -> None:
        delay = 1.0
        while True:
            try:
                await self._listen()
            except asyncio.CancelledError:
                raise
            except (StoreUnavailable, *REDIS_DOWN) as error:
                log.warning("Live feed lost Redis (%s); reconnecting in %.0f s", error, delay)
            except Exception:
                log.exception("Live feed stopped unexpectedly; restarting in %.0f s", delay)
            if self._was_connected:
                delay = 1.0  # it worked for a while: start counting again from 1 s
                self._was_connected = False
            LIVE_FEED_UP.set(0)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    async def _listen(self) -> None:
        async with self.store.redis.pubsub() as pubsub:
            await pubsub.psubscribe(*self.patterns)
            self._was_connected = True
            LIVE_FEED_UP.set(1)
            log.info("Live feed listening on %s", ", ".join(self.patterns))
            async for message in pubsub.listen():
                if message["type"] != "pmessage":
                    continue  # the confirmation of our own subscribe
                key = message["channel"].removeprefix(self.channel_prefix)
                await self.handle_change(key)

    async def handle_change(self, key: str) -> None:
        """Read the changed key and push its new value. A deleted key is ignored.

        If Redis is down, get_document raises StoreUnavailable and run() reconnects.
        """
        kind = parse_key(key)
        if kind is None:
            return
        document = await self.store.get_document(key)
        if document is None:
            return  # deleted or expired: nothing to show
        if kind[0] == "ship":
            ships = parse_documents(Ship, [document], store="redis")
            if ships:
                ship = ships[0]
                await self.connections.broadcast(
                    {"type": "ship", "ship": ship.model_dump(mode="json")}, port=ship.port
                )
                PUSHED.labels(kind="ship").inc()
        else:
            port = document.get("port") or kind[1]
            slots = parse_documents(OutlookSlot, [document], store="redis")
            if slots:
                await self.connections.broadcast(
                    {"type": "outlook", "port": port, "slot": slots[0].model_dump(mode="json")},
                    port=port,
                )
                PUSHED.labels(kind="outlook").inc()
