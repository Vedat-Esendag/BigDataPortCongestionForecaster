"""Map aisstream.io messages to records for the ais.position and ais.static topics.

Pure functions, no I/O, so they are easy to test. Values are mapped faithfully: AIS "not
available" codes (sog 102.3, heading 511, ...) are NOT turned into nulls here. Cleaning is the
job of spark/common/clean.py (F4), so raw history stays raw.

aisstream message shape (https://aisstream.io/documentation):
    {"MessageType": "PositionReport",
     "MetaData": {"MMSI": 219000001, "time_utc": "2026-10-01 08:00:00.123456789 +0000 UTC", ...},
     "Message": {"PositionReport": {"Latitude": .., "Longitude": .., "Sog": .., ...}}}
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

POSITION_TOPIC = "ais.position"
STATIC_TOPIC = "ais.static"

# aisstream sends Go-style timestamps with up to 9 fractional digits and a trailing " UTC".
_TIME_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}) (?P<clock>\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d+))?"
)

# Position messages: Class A (1, 2, 3), Class B (18), Class B extended (19)
_POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}


@dataclass(frozen=True)
class Mapped:
    """One record ready for Kafka: topic, string key (the MMSI), and the Avro value dict."""

    topic: str
    key: str
    value: dict[str, Any]


def parse_time_utc(text: str) -> datetime:
    """Parse aisstream's MetaData.time_utc into an aware UTC datetime."""
    match = _TIME_RE.match(text.strip())
    if not match:
        raise ValueError(f"unrecognised time_utc: {text!r}")
    fraction = (match["frac"] or "0")[:6].ljust(6, "0")
    naive = datetime.strptime(
        f"{match['date']} {match['clock']}.{fraction}", "%Y-%m-%d %H:%M:%S.%f"
    )
    return naive.replace(tzinfo=UTC)


def to_millis(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def infer_eta(eta: dict[str, int] | None, reference: datetime) -> datetime | None:
    """Turn the AIS ETA (month, day, hour, minute, no year) into a full timestamp.

    AIS codes "not available" as month 0, day 0, hour 24 and minute 60. The year is taken from
    `reference`; an ETA more than 30 days before `reference` is assumed to be next year's.
    """
    if not eta:
        return None
    month, day = eta.get("Month", 0), eta.get("Day", 0)
    hour, minute = eta.get("Hour", 24), eta.get("Minute", 60)
    if not (1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    try:
        candidate = datetime(reference.year, month, day, hour, minute, tzinfo=UTC)
        if candidate < reference - timedelta(days=30):
            candidate = candidate.replace(year=reference.year + 1)
    except ValueError:  # for example 31 April, or 29 February in a non-leap year
        return None
    return candidate


def _clean_text(value: Any) -> str | None:
    """AIS pads text fields with '@' or spaces; empty after stripping means unknown."""
    if not isinstance(value, str):
        return None
    text = value.strip().rstrip("@").strip()
    return text or None


def _positive(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) and value > 0 else None


def map_position(envelope: dict[str, Any], received: datetime) -> Mapped:
    body = envelope["Message"][envelope["MessageType"]]
    meta = envelope["MetaData"]
    mmsi = int(meta["MMSI"])
    return Mapped(
        topic=POSITION_TOPIC,
        key=str(mmsi),
        value={
            "mmsi": mmsi,
            "event_time": to_millis(parse_time_utc(meta["time_utc"])),
            "received_time": to_millis(received),
            "lat": float(body["Latitude"]),
            "lon": float(body["Longitude"]),
            "sog": body.get("Sog"),
            "cog": body.get("Cog"),
            "heading": body.get("TrueHeading"),
            "rot": body.get("RateOfTurn"),
            "nav_status": body.get("NavigationalStatus"),
            "message_type": body.get("MessageID"),
            "source": "aisstream",
        },
    )


def map_static(envelope: dict[str, Any], received: datetime) -> Mapped:
    body = envelope["Message"]["ShipStaticData"]
    meta = envelope["MetaData"]
    mmsi = int(meta["MMSI"])
    event_time = parse_time_utc(meta["time_utc"])

    dimension = body.get("Dimension") or {}
    length = (dimension.get("A") or 0) + (dimension.get("B") or 0)
    beam = (dimension.get("C") or 0) + (dimension.get("D") or 0)
    eta = infer_eta(body.get("Eta"), event_time)
    imo = body.get("ImoNumber")

    return Mapped(
        topic=STATIC_TOPIC,
        key=str(mmsi),
        value={
            "mmsi": mmsi,
            "event_time": to_millis(event_time),
            "received_time": to_millis(received),
            "imo": int(imo) if imo else None,
            "call_sign": _clean_text(body.get("CallSign")),
            "name": _clean_text(body.get("Name")) or _clean_text(meta.get("ShipName")),
            "ship_type": body.get("Type"),
            "length_m": _positive(length),
            "beam_m": _positive(beam),
            "draught_m": _positive(body.get("MaximumStaticDraught")),
            "destination": _clean_text(body.get("Destination")),
            "eta_broadcast": to_millis(eta) if eta else None,
            "source": "aisstream",
        },
    )


def map_message(envelope: dict[str, Any], received: datetime | None = None) -> Mapped | None:
    """Map one aisstream envelope, or return None for message types we do not use."""
    received = received or datetime.now(UTC)
    message_type = envelope.get("MessageType")
    if message_type in _POSITION_TYPES:
        return map_position(envelope, received)
    if message_type == "ShipStaticData":
        return map_static(envelope, received)
    return None
