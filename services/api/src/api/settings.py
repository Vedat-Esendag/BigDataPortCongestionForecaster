"""Settings from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class Settings:
    redis_url: str
    cassandra_contact_points: tuple[str, ...]
    cassandra_port: int
    cassandra_keyspace: str
    cassandra_local_dc: str
    ports_config: Path
    cors_origins: tuple[str, ...]
    # A ship whose last prediction is this much older than the newest prediction of any ship is
    # treated as gone (berthed or sailed away) and left off the ship list. See drop_departed in
    # clients/redis_client.py.
    ship_max_age: timedelta


def load_settings() -> Settings:
    ports_config = os.environ.get("PORTS_CONFIG")
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        cassandra_contact_points=tuple(
            os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
        ),
        cassandra_port=int(os.environ.get("CASSANDRA_PORT", "9042")),
        cassandra_keyspace=os.environ.get("CASSANDRA_KEYSPACE", "portcongestion"),
        cassandra_local_dc=os.environ.get("CASSANDRA_LOCAL_DC", "datacenter1"),
        ports_config=Path(ports_config) if ports_config else _REPO_ROOT / "config" / "ports.yaml",
        cors_origins=tuple(
            os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://localhost:8080").split(",")
        ),
        ship_max_age=timedelta(minutes=int(os.environ.get("SHIP_MAX_AGE_MINUTES", "360"))),
    )
