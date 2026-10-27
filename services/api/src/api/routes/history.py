from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from api.models import OutlookSlot, Ship
from api.ports import require_port

router = APIRouter(prefix="/history", tags=["history"])

DayQuery = Annotated[str, Query(pattern=r"^\d{4}-\d{2}-\d{2}$", description="UTC day, yyyy-MM-dd")]


@router.get("/ships", response_model=list[Ship])
async def ship_history(request: Request, port: str, day: DayQuery) -> list[Ship]:
    """Every stored prediction for `port` on `day`, newest first.

    TODO(F10): SELECT from ship_prediction_by_port_day WHERE port = ? AND day = ?
    """
    require_port(request.app.state.ports, port)
    return []


@router.get("/outlook", response_model=list[OutlookSlot])
async def outlook_history(request: Request, port: str, day: DayQuery) -> list[OutlookSlot]:
    """Every stored outlook computation for `port` on `day`, newest first.

    TODO(F10): SELECT from port_outlook_by_port_day WHERE port = ? AND day = ?
    """
    require_port(request.app.state.ports, port)
    return []
