from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. Does not touch Redis or Cassandra."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    """Readiness: should this pod get traffic? 200 if it can reach Redis, 503 if not.

    Only Redis decides. Without Redis the pod can serve neither the live view nor the push, so
    Kubernetes should send dashboards to another pod. Without Cassandra the live view still
    works and only history is missing, which is exactly the degraded mode the project wants,
    so the pod stays in service. The Cassandra status is reported for information.
    """
    redis_ok = await request.app.state.live.ping()
    body = {"redis": redis_ok, "cassandra": request.app.state.history.is_connected()}
    return JSONResponse(body, status_code=200 if redis_ok else 503)
