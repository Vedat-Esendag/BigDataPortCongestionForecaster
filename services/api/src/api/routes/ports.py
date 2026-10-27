from __future__ import annotations

from fastapi import APIRouter, Request

from api.models import PortInfo

router = APIRouter(tags=["ports"])


@router.get("/ports", response_model=list[PortInfo])
async def list_ports(request: Request) -> list[PortInfo]:
    """The configured ports. The dashboard centers its map on the first one."""
    return request.app.state.ports
