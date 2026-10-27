from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["live"])


@router.websocket("/ws")
async def live(socket: WebSocket) -> None:
    """Live push channel for the dashboard.

    Protocol (stub): the server sends {"type": "hello", "ports": [...]} on connect and answers
    the text "ping" with "pong". TODO(F10): after hello, push {"type": "ship", ...} and
    {"type": "outlook", ...} messages as Redis announces new state (see api/live.py).
    """
    manager = socket.app.state.connections
    await manager.connect(socket)
    try:
        ports = [port.id for port in socket.app.state.ports]
        await socket.send_json({"type": "hello", "ports": ports})
        while True:
            message = await socket.receive_text()
            if message == "ping":
                await socket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(socket)
