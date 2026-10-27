"""Response models. They are the contract with the dashboard (services/dashboard/src/types.ts)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


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
    """Latest prediction for one ship (the ship.prediction topic)."""

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
