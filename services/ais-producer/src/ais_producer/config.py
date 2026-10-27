"""Settings from environment variables and the port bounding boxes from config/ports.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

# Walk up from this file to find the repo root when running from a checkout.
_REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class Settings:
    api_key: str
    bootstrap_servers: str
    schema_registry_url: str
    ports_config: Path
    schema_dir: Path
    metrics_port: int
    auto_register_schemas: bool
    websocket_url: str = "wss://stream.aisstream.io/v0/stream"
    position_topic: str = "ais.position"
    static_topic: str = "ais.static"


def _path(env_name: str, default: Path) -> Path:
    value = os.environ.get(env_name)
    path = Path(value) if value else default
    return path if path.is_absolute() else Path.cwd() / path


def load_settings() -> Settings:
    """Read settings from the environment. Raises SystemExit when the API key is missing."""
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("AISSTREAM_API_KEY is not set. Put it in .env (see .env.example).")
    return Settings(
        api_key=api_key,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        ports_config=_path("PORTS_CONFIG", _REPO_ROOT / "config" / "ports.yaml"),
        schema_dir=_path("AVRO_SCHEMA_DIR", _REPO_ROOT / "schemas" / "avro"),
        metrics_port=int(os.environ.get("PRODUCER_METRICS_PORT", "9100")),
        auto_register_schemas=os.environ.get("SCHEMA_AUTO_REGISTER", "false").lower() == "true",
    )


def load_bounding_boxes(ports_config: Path) -> list[list[list[float]]]:
    """One aisstream box per port, in aisstream's [[lat, lon], [lat, lon]] corner format."""
    ports = yaml.safe_load(ports_config.read_text())["ports"]
    boxes = []
    for port in ports:
        box = port["bbox"]
        boxes.append([[box["min_lat"], box["min_lon"]], [box["max_lat"], box["max_lon"]]])
    return boxes
