"""Settings from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class Settings:
    redis_url: str
    cassandra_contact_points: tuple[str, ...]
    cassandra_port: int
    cassandra_keyspace: str
    ports_config: Path
    cors_origins: tuple[str, ...]


def load_settings() -> Settings:
    ports_config = os.environ.get("PORTS_CONFIG")
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        cassandra_contact_points=tuple(
            os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
        ),
        cassandra_port=int(os.environ.get("CASSANDRA_PORT", "9042")),
        cassandra_keyspace=os.environ.get("CASSANDRA_KEYSPACE", "portcongestion"),
        ports_config=Path(ports_config) if ports_config else _REPO_ROOT / "config" / "ports.yaml",
        cors_origins=tuple(
            os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://localhost:8080").split(",")
        ),
    )
