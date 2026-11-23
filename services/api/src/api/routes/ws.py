from __future__ import annotations

import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["live"])


@router.websocket("/ws")
async def live(socket: WebSocket) -> None:
    """Live push channel for the dashboard (F10).

    Server to client:
      {"type": "hello", "ports": [...]}                 once, right after connecting
      {"type": "ship", "ship": {...}}                   a ship got a new prediction
      {"type": "outlook", "port": "...", "slot": {...}} a port got a new outlook slot
      {"type": "subscribed", "port": "..." | null}      answer to a subscribe
      {"type": "error", "detail": "..."}                a message from the client was not understood

    Client to server:
      "ping"                                  answered with "pong" (keep-alive)
      {"type": "subscribe", "port": "dkebj"}  only send changes for this port
      {"type": "subscribe", "port": null}     send changes for every port (the default)

    The pushes come from LiveFeed in api/live.py. A client that hasn't subscribed gets every
    port, which is what the current dashboard expects (it filters by port itself).
    """
    manager = socket.app.state.connections
    port_ids = [port.id for port in socket.app.state.ports]
    await manager.connect(socket)
    try:
        await socket.send_json({"type": "hello", "ports": port_ids})
        while True:
            text = await socket.receive_text()
            if text == "ping":
                await socket.send_text("pong")
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError:
                message = None
            if isinstance(message, dict) and message.get("type") == "subscribe":
                port = message.get("port")
                if port is None or port in port_ids:
                    manager.subscribe(socket, port)
                    await socket.send_json({"type": "subscribed", "port": port})
                    continue
                await socket.send_json({"type": "error", "detail": f"unknown port: {port}"})
                continue
            await socket.send_json({"type": "error", "detail": "expected 'ping' or a subscribe"})
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(socket)
