from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from api.models import Ship
from api.ports import require_port

router = APIRouter(tags=["ships"])


@router.get("/ships", response_model=list[Ship])
async def list_ships(request: Request, port: str) -> list[Ship]:
    """Latest prediction of every ship heading for `port`.

    TODO(F10): read the ship.prediction:<mmsi> documents from Redis (SCAN, then JSON.MGET) and
               keep those whose `port` matches. Replace the SCAN with a secondary index if the
               number of ships makes it slow.
    """
    require_port(request.app.state.ports, port)
    return []


@router.get("/ships/{mmsi}", response_model=Ship)
async def get_ship(request: Request, mmsi: int) -> Ship:
    """Latest prediction of one ship. TODO(F10): JSON.GET ship.prediction:<mmsi> from Redis."""
    raise HTTPException(status_code=404, detail=f"no prediction for ship {mmsi}")
