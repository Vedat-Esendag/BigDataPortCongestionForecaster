from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import fastavro
import pytest

from ais_producer.mapping import infer_eta, map_message, parse_time_utc

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "avro"
RECEIVED = datetime(2026, 10, 1, 8, 0, 1, tzinfo=UTC)


def millis(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def position_envelope(**overrides):
    body = {
        "Cog": 270.5,
        "Latitude": 56.15,
        "Longitude": 10.25,
        "MessageID": 1,
        "NavigationalStatus": 0,
        "RateOfTurn": 0,
        "Sog": 12.3,
        "TrueHeading": 268,
        "UserID": 219000001,
    }
    body.update(overrides)
    return {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": 219000001,
            "ShipName": "TEST VESSEL  ",
            "time_utc": "2026-10-01 08:00:00.123456789 +0000 UTC",
        },
        "Message": {"PositionReport": body},
    }


def static_envelope(**overrides):
    body = {
        "CallSign": "OZ1234 ",
        "Destination": "ESBJERG@@@@@@@@@@@@@@",
        "Dimension": {"A": 120, "B": 30, "C": 10, "D": 12},
        "Eta": {"Day": 2, "Hour": 12, "Minute": 30, "Month": 10},
        "ImoNumber": 9876543,
        "MaximumStaticDraught": 7.5,
        "Name": "TEST VESSEL@@@@@@@@@",
        "Type": 70,
    }
    body.update(overrides)
    return {
        "MessageType": "ShipStaticData",
        "MetaData": {
            "MMSI": 219000001,
            "ShipName": "TEST VESSEL",
            "time_utc": "2026-10-01 08:00:00.5 +0000 UTC",
        },
        "Message": {"ShipStaticData": body},
    }


def test_position_report_goes_to_ais_position_keyed_by_mmsi():
    mapped = map_message(position_envelope(), RECEIVED)

    assert mapped.topic == "ais.position"
    assert mapped.key == "219000001"
    assert mapped.value == {
        "mmsi": 219000001,
        "event_time": millis(2026, 10, 1, 8, 0, 0) + 123,
        "received_time": millis(2026, 10, 1, 8, 0, 1),
        "lat": 56.15,
        "lon": 10.25,
        "sog": 12.3,
        "cog": 270.5,
        "heading": 268,
        "rot": 0,
        "nav_status": 0,
        "message_type": 1,
        "source": "aisstream",
    }


def test_ship_static_data_goes_to_ais_static_with_derived_fields():
    mapped = map_message(static_envelope(), RECEIVED)

    assert mapped.topic == "ais.static"
    assert mapped.key == "219000001"
    value = mapped.value
    assert value["name"] == "TEST VESSEL"
    assert value["call_sign"] == "OZ1234"
    assert value["destination"] == "ESBJERG"
    assert value["imo"] == 9876543
    assert value["ship_type"] == 70
    assert value["length_m"] == 150.0
    assert value["beam_m"] == 22.0
    assert value["draught_m"] == 7.5
    assert value["eta_broadcast"] == millis(2026, 10, 2, 12, 30)


def test_unavailable_static_values_become_none():
    envelope = static_envelope(
        Dimension={"A": 0, "B": 0, "C": 0, "D": 0},
        Eta={"Day": 0, "Hour": 24, "Minute": 60, "Month": 0},
        ImoNumber=0,
        MaximumStaticDraught=0,
        Destination="@@@@@@@@@@@@@@@@@@@@",
    )

    value = map_message(envelope, RECEIVED).value

    assert value["eta_broadcast"] is None
    assert value["imo"] is None
    assert value["length_m"] is None
    assert value["draught_m"] is None
    assert value["destination"] is None


def test_sentinel_codes_are_kept_for_the_cleaning_step():
    value = map_message(position_envelope(Sog=102.3, TrueHeading=511, Cog=360), RECEIVED).value

    assert (value["sog"], value["heading"], value["cog"]) == (102.3, 511, 360)


def test_other_message_types_are_ignored():
    envelope = {"MessageType": "BaseStationReport", "MetaData": {}, "Message": {}}

    assert map_message(envelope, RECEIVED) is None


def test_a_broken_message_raises_instead_of_producing_garbage():
    envelope = position_envelope()
    del envelope["Message"]["PositionReport"]["Latitude"]

    with pytest.raises(KeyError):
        map_message(envelope, RECEIVED)


def test_parse_time_utc_handles_go_style_fractions():
    assert parse_time_utc("2026-10-01 08:00:00 +0000 UTC") == datetime(2026, 10, 1, 8, tzinfo=UTC)
    assert parse_time_utc("2026-10-01 08:00:00.5 +0000 UTC").microsecond == 500000
    assert parse_time_utc("2026-10-01 08:00:00.123456789 +0000 UTC").microsecond == 123456
    with pytest.raises(ValueError):
        parse_time_utc("yesterday")


def test_eta_year_rolls_over_when_the_date_has_passed():
    reference = datetime(2026, 12, 28, 10, 0, tzinfo=UTC)

    assert infer_eta({"Month": 1, "Day": 3, "Hour": 6, "Minute": 0}, reference) == datetime(
        2027, 1, 3, 6, 0, tzinfo=UTC
    )
    # a few days in the past is kept in the same year (ship is running late)
    assert infer_eta({"Month": 12, "Day": 25, "Hour": 6, "Minute": 0}, reference) == datetime(
        2026, 12, 25, 6, 0, tzinfo=UTC
    )
    # impossible calendar date
    assert infer_eta({"Month": 2, "Day": 30, "Hour": 6, "Minute": 0}, reference) is None


@pytest.mark.parametrize(
    ("envelope_factory", "schema_file"),
    [(position_envelope, "ais.position.avsc"), (static_envelope, "ais.static.avsc")],
)
def test_mapped_values_match_the_avro_schema(envelope_factory, schema_file):
    schema = json.loads((SCHEMA_DIR / schema_file).read_text())
    value = map_message(envelope_factory(), RECEIVED).value

    assert set(value) == {field["name"] for field in schema["fields"]}
    assert fastavro.validate(value, fastavro.parse_schema(schema), raise_errors=True)
