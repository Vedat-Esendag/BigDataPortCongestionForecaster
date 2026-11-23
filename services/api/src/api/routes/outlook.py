"""The port outlook right now. Read from Redis (latest state)."""

from __future__ import annotations

from fastapi import APIRouter, Request

from api.models import Outlook
from api.ports import require_port

router = APIRouter(tags=["outlook"])


@router.get("/outlook", response_model=Outlook)
async def get_outlook(request: Request, port: str) -> Outlook:
    """The current outlook of `port`, one entry per time slot, earliest slot first.

    Until the Redis key includes the slot (docs/storage.md, known gaps), this returns at most
    one slot: the most recently computed one. 503 if Redis is down.
    """
    require_port(request.app.state.ports, port)
    slots = await request.app.state.live.outlook_for_port(port)
    return Outlook(port=port, slots=slots)
