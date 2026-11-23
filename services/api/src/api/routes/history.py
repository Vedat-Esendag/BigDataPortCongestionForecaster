"""What happened on a given day. Read from Cassandra (history)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from api.models import OutlookSlot, Ship
from api.ports import require_port

router = APIRouter(prefix="/history", tags=["history"])

DayQuery = Annotated[str, Query(pattern=r"^\d{4}-\d{2}-\d{2}$", description="UTC day, yyyy-MM-dd")]
# One day of one port is one Cassandra partition, so even the maximum is a single cheap read.
# 5000 is also the driver's page size; the client returns only the first page.
LimitQuery = Annotated[int, Query(ge=1, le=5000, description="Most rows to return, newest first")]


@router.get("/ships", response_model=list[Ship])
async def ship_history(
    request: Request, port: str, day: DayQuery, limit: LimitQuery = 1000
) -> list[Ship]:
    """Every stored prediction for `port` on `day`, newest first. 503 if Cassandra is down."""
    require_port(request.app.state.ports, port)
    return await request.app.state.history.ship_history(port, day, limit)


@router.get("/outlook", response_model=list[OutlookSlot])
async def outlook_history(
    request: Request, port: str, day: DayQuery, limit: LimitQuery = 1000
) -> list[OutlookSlot]:
    """Every stored outlook computation for `port` on `day`, newest first. 503 if Cassandra
    is down."""
    require_port(request.app.state.ports, port)
    return await request.app.state.history.outlook_history(port, day, limit)
