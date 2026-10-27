from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. Does not touch Redis or Cassandra."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> dict[str, bool]:
    """Readiness: can this instance reach Redis? (TODO(F10): also check Cassandra.)"""
    try:
        redis_ok = bool(await request.app.state.redis.ping())
    except Exception:
        redis_ok = False
    return {"redis": redis_ok}
