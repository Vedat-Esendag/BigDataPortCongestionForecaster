"""The ships around a port right now. Read from Redis (latest state)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from api.models import Ship
from api.ports import require_port

router = APIRouter(tags=["ships"])


@router.get("/ships", response_model=list[Ship])
async def list_ships(request: Request, port: str) -> list[Ship]:
    """Latest prediction of every ship heading for `port`, earliest predicted arrival first.

    Ships that stopped getting predictions long before the others (berthed or gone) are left
    out; see drop_departed in clients/redis_client.py. 503 if Redis is down.
    """
    require_port(request.app.state.ports, port)
    return await request.app.state.live.ships_for_port(port)


@router.get("/ships/{mmsi}", response_model=Ship)
async def get_ship(request: Request, mmsi: int) -> Ship:
    """Latest prediction of one ship. 404 if Redis has none for it."""
    ship = await request.app.state.live.get_ship(mmsi)
    if ship is None:
        raise HTTPException(status_code=404, detail=f"no prediction for ship {mmsi}")
    return ship
