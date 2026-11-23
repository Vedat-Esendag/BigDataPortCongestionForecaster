"""Response models. They are the contract with the dashboard (services/dashboard/src/types.ts).

The same models read both stores: Redis documents (JSON written by the Redis sink, timestamps
as epoch milliseconds) and Cassandra rows (timestamps as datetimes). Pydantic accepts both.
Extra fields, such as `day`, are ignored.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class PortInfo(BaseModel):
    id: str
    name: str
    country: str
    center: dict[str, float]
    bbox: dict[str, float]
    approach_zone: list[list[float]]
    anchorage: list[list[float]]
    berth_area: list[list[float]]


class Ship(BaseModel):
    """One prediction for one ship (the ship.prediction topic)."""

    mmsi: int
    port: str
    broadcast_eta: datetime | None
    predicted_eta: datetime
    eta_lower: datetime
    eta_upper: datetime
    model_version: str
    reason_codes: list[str]
    lat: float | None
    lon: float | None
    event_time: datetime
    # When Spark produced this prediction. The dashboard uses it to show how old the numbers
    # are. Optional so that older records without it still load.
    processing_time: datetime | None = None

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _empty_list_for_null(cls, value: Any) -> Any:
        # Cassandra stores an empty list as null, so a ship with no reasons comes back as None.
        return [] if value is None else value


class OutlookSlot(BaseModel):
    """One time slot of the port outlook (the port.outlook topic)."""

    slot_start: datetime
    slot_end: datetime
    expected_arrivals: int
    late_count: int
    bunching_flag: bool
    expected_waiting_ships: float
    computed_at: datetime


class Outlook(BaseModel):
    port: str
    slots: list[OutlookSlot]
