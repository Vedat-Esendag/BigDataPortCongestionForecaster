"""The F5, F6 and F8 stubs run and keep their output contract."""

from __future__ import annotations

from datetime import UTC, datetime

from common.config import load_ports
from common.features import build_features
from common.port_events import assign_destination_port, detect_port_events
from common.schemas import FEATURE_BASE_SCHEMA, PORT_EVENT_SCHEMA
from streaming.predictor import predict_eta

ETA = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
SEEN = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)

SQUARE = "[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]"


def _port_yaml(port_id: str, name: str, unlocode: str) -> str:
    """One minimal, valid ports.yaml entry. The project has one real port; tests invent more."""
    return f"""  - id: {port_id}
    name: {name}
    country: "DK"
    unlocode: {unlocode}
    center: {{lat: 0.5, lon: 0.5}}
    bbox: {{min_lat: 0, min_lon: 0, max_lat: 1, max_lon: 1}}
    approach_zone: {SQUARE}
    anchorage: {SQUARE}
    berth_area: {SQUARE}
    weather:
      open_meteo: {{lat: 0.5, lon: 0.5}}
      dmi_station_ids: []
"""


def _static(spark):
    return spark.createDataFrame(
        [
            (219000001, "Esbjerg", ETA, SEEN),
            (219000002, "DKEBJ", ETA, SEEN),
            (219000003, "SOMEWHERE", ETA, SEEN),
            (219000004, "ESBJERG", None, SEEN),
        ],
        "mmsi long, destination string, eta_broadcast timestamp, event_time timestamp",
    )


def test_detect_port_events_stub_returns_the_event_schema(ais_sample):
    events = detect_port_events(ais_sample, load_ports())

    assert events.schema == PORT_EVENT_SCHEMA
    assert events.count() == 0


def test_assign_destination_port_placeholder(spark):
    result = assign_destination_port(_static(spark), load_ports())

    ports = {row.mmsi: row.port for row in result.collect()}
    assert ports == {219000001: "dkebj", 219000002: "dkebj", 219000003: None, 219000004: "dkebj"}


def test_assign_destination_port_with_several_ports(spark, tmp_path):
    """Matches by name or UN/LOCODE; the port listed first wins when a text names two."""
    config = tmp_path / "ports.yaml"
    config.write_text(
        "ports:\n" + _port_yaml("alpha", "Alpha", "DKAAA") + _port_yaml("beta", "Beta", "DKBBB")
    )
    static = spark.createDataFrame(
        [(1, "alpha"), (2, "BETA bay"), (3, "DKBBB"), (4, "nowhere"), (5, None), (6, "ALPHA>BETA")],
        "mmsi long, destination string",
    )

    result = assign_destination_port(static, load_ports(config))

    ports = {row.mmsi: row.port for row in result.collect()}
    assert ports == {1: "alpha", 2: "beta", 3: "beta", 4: None, 5: None, 6: "alpha"}


def test_build_features_stub_has_the_base_columns(spark):
    static = assign_destination_port(_static(spark), load_ports())

    features = build_features(static)

    assert [f.name for f in features.schema] == [f.name for f in FEATURE_BASE_SCHEMA]


def test_dummy_predictor_copies_the_broadcast_eta(spark):
    static = assign_destination_port(_static(spark), load_ports())

    predictions = predict_eta(build_features(static)).collect()

    # ships without a port or without a broadcast ETA cannot be predicted by the placeholder
    assert sorted(p.mmsi for p in predictions) == [219000001, 219000002]
    for row in predictions:
        assert row.predicted_eta == row.broadcast_eta
        assert row.eta_lower < row.predicted_eta < row.eta_upper
        assert row.model_version == "dummy"
        assert row.reason_codes == ["dummy_predictor"]
        assert row.day == "2026-10-01"
