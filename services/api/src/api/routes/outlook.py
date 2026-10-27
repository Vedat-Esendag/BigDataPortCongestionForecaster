from __future__ import annotations

from fastapi import APIRouter, Request

from api.models import Outlook
from api.ports import require_port

router = APIRouter(tags=["outlook"])


@router.get("/outlook", response_model=Outlook)
async def get_outlook(request: Request, port: str) -> Outlook:
    """The current outlook of `port` for the next 24 to 48 hours, one entry per time slot.

    TODO(F10): read the latest port.outlook documents of the port from Redis.
    """
    require_port(request.app.state.ports, port)
    return Outlook(port=port, slots=[])
