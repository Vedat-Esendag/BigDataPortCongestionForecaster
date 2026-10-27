"""WebSocket connections and live push (F10)."""

from __future__ import annotations

import json
from typing import Any

from fastapi import WebSocket
from prometheus_client import Gauge

WS_CONNECTIONS = Gauge("api_websocket_connections", "Open dashboard WebSocket connections")


class ConnectionManager:
    """Keeps the open sockets of this API instance. Each instance pushes to its own clients."""

    def __init__(self) -> None:
        self._sockets: set[WebSocket] = set()

    async def connect(self, socket: WebSocket) -> None:
        await socket.accept()
        self._sockets.add(socket)
        WS_CONNECTIONS.set(len(self._sockets))

    def disconnect(self, socket: WebSocket) -> None:
        self._sockets.discard(socket)
        WS_CONNECTIONS.set(len(self._sockets))

    async def broadcast(self, message: dict[str, Any]) -> None:
        """Send one JSON message to every client, dropping clients that fail."""
        text = json.dumps(message, default=str)
        for socket in list(self._sockets):
            try:
                await socket.send_text(text)
            except Exception:
                self.disconnect(socket)


# TODO(F10): start a background task per API instance that psubscribes to the Redis keyspace
#            events of ship.prediction:* and port.outlook:* (the Redis sink cannot PUBLISH, so
#            Redis announces each write itself, see docs/connectors.md), reads the changed key,
#            and calls ConnectionManager.broadcast. Filter by the port each client subscribed to.
