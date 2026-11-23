"""Shared test data and fake stores.

The fakes stand in for Redis and Cassandra so the route tests run without either.
test_integration.py runs the same API against the real ones.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from redis.asyncio import Redis

from api.clients.redis_client import LiveStore
from api.errors import StoreUnavailable, parse_documents
from api.models import OutlookSlot, Ship

# 2026-11-16 13:00:00 UTC in epoch milliseconds, the way the Redis sink writes timestamps.
T0 = 1_794_834_000_000
MINUTE = 60_000
HOUR = 60 * MINUTE


def ship_doc(mmsi: int, port: str = "dkebj", processed_at: int = T0, **changes: Any) -> dict:
    """A ship.prediction document as the Redis sink stores it."""
    doc = {
        "mmsi": mmsi,
        "port": port,
        "broadcast_eta": T0 + 2 * HOUR,
        "predicted_eta": T0 + 3 * HOUR,
        "eta_lower": T0 + 2 * HOUR + 30 * MINUTE,
        "eta_upper": T0 + 3 * HOUR + 30 * MINUTE,
        "model_version": "v3",
        "reason_codes": ["high_wind"],
        "lat": 55.41,
        "lon": 8.12,
        "event_time": processed_at - 10_000,
        "processing_time": processed_at,
        "day": "2026-11-16",
    }
    doc.update(changes)
    return doc


def outlook_doc(port: str = "dkebj", slot_start: int = T0 + HOUR, **changes: Any) -> dict:
    """A port.outlook document as the Redis sink stores it."""
    doc = {
        "port": port,
        "slot_start": slot_start,
        "slot_end": slot_start + HOUR,
        "expected_arrivals": 4,
        "late_count": 1,
        "bunching_flag": True,
        "expected_waiting_ships": 1.5,
        "computed_at": T0,
        "day": "2026-11-16",
    }
    doc.update(changes)
    return doc


class FakeLiveStore(LiveStore):
    """LiveStore with fixed contents instead of Redis.

    It still holds a real Redis client, pointed at a port where nothing listens, because the
    app's live feed starts on it at startup. That feed just fails quietly and retries.
    """

    def __init__(
        self, ships: list[dict] | None = None, slots: list[dict] | None = None, down: bool = False
    ) -> None:
        super().__init__(Redis.from_url("redis://127.0.0.1:1/0"), timedelta(hours=6))
        self.ships = ships or []
        self.slots = slots or []
        self.down = down

    def _check(self) -> None:
        if self.down:
            raise StoreUnavailable("redis", "fake outage")

    async def ping(self) -> bool:
        return not self.down

    async def get_ship(self, mmsi: int) -> Ship | None:
        self._check()
        found = parse_documents(Ship, [d for d in self.ships if d["mmsi"] == mmsi], "redis")
        return found[0] if found else None

    async def ships_for_port(self, port: str) -> list[Ship]:
        self._check()
        return parse_documents(Ship, [d for d in self.ships if d["port"] == port], "redis")

    async def outlook_for_port(self, port: str) -> list[OutlookSlot]:
        self._check()
        return parse_documents(OutlookSlot, [d for d in self.slots if d["port"] == port], "redis")


class FakeHistory:
    """Stands in for CassandraClient."""

    def __init__(
        self, ships: list[dict] | None = None, slots: list[dict] | None = None, down: bool = False
    ) -> None:
        self.ships = ships or []
        self.slots = slots or []
        self.down = down
        self.calls: list[tuple] = []

    def try_connect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return not self.down

    def close(self) -> None:
        pass

    async def ship_history(self, port: str, day: str, limit: int) -> list[Ship]:
        self.calls.append(("ships", port, day, limit))
        if self.down:
            raise StoreUnavailable("cassandra", "fake outage")
        return parse_documents(Ship, self.ships[:limit], "cassandra")

    async def outlook_history(self, port: str, day: str, limit: int) -> list[OutlookSlot]:
        self.calls.append(("outlook", port, day, limit))
        if self.down:
            raise StoreUnavailable("cassandra", "fake outage")
        return parse_documents(OutlookSlot, self.slots[:limit], "cassandra")
