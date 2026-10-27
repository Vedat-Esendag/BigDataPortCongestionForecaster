"""Load config/ports.yaml and config/thresholds.yaml, and resolve data lake and model paths.

Where the YAML files are found, in order:
  1. the CONFIG_DIR environment variable (required when this package runs from a zip, as it does
     under spark-submit --py-files, because __file__ then points inside the zip)
  2. the repo's config/ directory next to spark/
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

# (lon, lat) pairs in GeoJSON order
Polygon = tuple[tuple[float, float], ...]

LAKE_ZONES = ("landing", "raw", "standardized", "curated", "quarantine")


@dataclass(frozen=True)
class BBox:
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float


@dataclass(frozen=True)
class Port:
    id: str
    name: str
    country: str
    unlocode: str
    center_lat: float
    center_lon: float
    bbox: BBox
    approach_zone: Polygon
    anchorage: Polygon
    berth_area: Polygon
    open_meteo_lat: float
    open_meteo_lon: float
    dmi_station_ids: tuple[str, ...]


@dataclass(frozen=True)
class AlertLevel:
    late_count: int
    expected_waiting_ships: int


@dataclass(frozen=True)
class Thresholds:
    late_minutes: int
    approach_radius_nm: float
    window_coverage: float
    slot_minutes: int
    horizons_hours: tuple[int, ...]
    bunching_min_arrivals: int
    warning: AlertLevel
    critical: AlertLevel


def config_dir() -> Path:
    override = os.environ.get("CONFIG_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "config"


def _polygon(raw: list[list[float]], label: str) -> Polygon:
    points = tuple((float(lon), float(lat)) for lon, lat in raw)
    if len(points) < 4 or points[0] != points[-1]:
        raise ValueError(f"{label}: polygon must have 3+ distinct points and be closed")
    return points


def load_ports(path: Path | None = None) -> list[Port]:
    """Read every port from ports.yaml and validate its polygons."""
    path = path or config_dir() / "ports.yaml"
    raw = yaml.safe_load(path.read_text())["ports"]
    ports = []
    for item in raw:
        pid = item["id"]
        weather = item["weather"]
        ports.append(
            Port(
                id=pid,
                name=item["name"],
                country=item["country"],
                unlocode=item["unlocode"],
                center_lat=float(item["center"]["lat"]),
                center_lon=float(item["center"]["lon"]),
                bbox=BBox(**{k: float(v) for k, v in item["bbox"].items()}),
                approach_zone=_polygon(item["approach_zone"], f"{pid}.approach_zone"),
                anchorage=_polygon(item["anchorage"], f"{pid}.anchorage"),
                berth_area=_polygon(item["berth_area"], f"{pid}.berth_area"),
                open_meteo_lat=float(weather["open_meteo"]["lat"]),
                open_meteo_lon=float(weather["open_meteo"]["lon"]),
                dmi_station_ids=tuple(str(s) for s in weather.get("dmi_station_ids", [])),
            )
        )
    return ports


def get_port(ports: list[Port], port_id: str) -> Port:
    for port in ports:
        if port.id == port_id:
            return port
    raise KeyError(f"unknown port id: {port_id}")


def load_thresholds(path: Path | None = None) -> Thresholds:
    path = path or config_dir() / "thresholds.yaml"
    raw = yaml.safe_load(path.read_text())
    return Thresholds(
        late_minutes=int(raw["late"]["minutes"]),
        approach_radius_nm=float(raw["approach_zone"]["radius_nm"]),
        window_coverage=float(raw["prediction_window"]["coverage"]),
        slot_minutes=int(raw["outlook"]["slot_minutes"]),
        horizons_hours=tuple(int(h) for h in raw["outlook"]["horizons_hours"]),
        bunching_min_arrivals=int(raw["outlook"]["bunching_min_arrivals"]),
        warning=AlertLevel(**raw["alerts"]["warning"]),
        critical=AlertLevel(**raw["alerts"]["critical"]),
    )


def lake_path(zone: str, dataset: str = "") -> str:
    """Full HDFS URI of a data lake zone, for example lake_path("curated", "features")."""
    if zone not in LAKE_ZONES:
        raise ValueError(f"unknown lake zone {zone!r}, expected one of {LAKE_ZONES}")
    base = os.environ.get("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
    root = os.environ.get("LAKE_ROOT", "/lake").rstrip("/")
    return "/".join(part for part in (f"{base}{root}", zone, dataset) if part)


def model_root() -> str:
    """HDFS URI of the directory that holds one v=<timestamp> folder per trained model."""
    base = os.environ.get("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
    return f"{base}{os.environ.get('MODEL_ROOT', '/models/eta')}"
