"""The Avro files in schemas/avro are the contract between all components."""

from __future__ import annotations

import json
from pathlib import Path

import fastavro
import pytest

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "avro"
TOPICS = [
    "ais.position",
    "ais.static",
    "ais.history.raw",
    "ais.history.position",
    "ais.history.static",
    "weather.forecast",
    "weather.observation",
    "ship.prediction",
    "port.outlook",
    "alerts",
    "analytics.weather_delay",
    "eval.metrics",
]


def test_there_is_one_schema_file_per_topic():
    assert sorted(p.stem for p in SCHEMA_DIR.glob("*.avsc")) == sorted(TOPICS)


@pytest.mark.parametrize("topic", TOPICS)
def test_schema_is_valid_avro(topic):
    schema = json.loads((SCHEMA_DIR / f"{topic}.avsc").read_text())

    fastavro.parse_schema(schema)
    assert schema["namespace"] == "com.sdu.portcongestion"


@pytest.mark.parametrize("kind", ["position", "static"])
def test_history_schemas_are_identical_to_the_live_ones(kind):
    live = json.loads((SCHEMA_DIR / f"ais.{kind}.avsc").read_text())
    history = json.loads((SCHEMA_DIR / f"ais.history.{kind}.avsc").read_text())

    assert live == history
