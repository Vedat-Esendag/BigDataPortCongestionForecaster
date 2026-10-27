"""Round trip through the Confluent wire format helpers, without Kafka."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import functions as F

from common.avro_io import decode_confluent_avro, encode_confluent_avro, field_names, load_avsc

SCHEMA_ID = 42
EVENT = datetime(2026, 10, 1, 8, 0, 0, tzinfo=UTC)
ETA = datetime(2026, 10, 2, 12, 30, 0, tzinfo=UTC)


def _prediction_row(spark):
    return spark.createDataFrame(
        [
            (
                219000001,
                "dkebj",
                ETA,
                ETA,
                ETA,
                ETA,
                "dummy",
                ["dummy_predictor"],
                56.15,
                10.25,
                EVENT,
                EVENT,
                "2026-10-01",
            )
        ],
        "mmsi long, port string, broadcast_eta timestamp, predicted_eta timestamp, "
        "eta_lower timestamp, eta_upper timestamp, model_version string, "
        "reason_codes array<string>, lat double, lon double, event_time timestamp, "
        "processing_time timestamp, day string",
    )


def test_value_starts_with_the_confluent_header(spark):
    avsc = load_avsc("ship.prediction")

    encoded = encode_confluent_avro(_prediction_row(spark), avsc, SCHEMA_ID, key_col="mmsi")
    row = encoded.collect()[0]

    assert row.key == "219000001"
    assert bytes(row.value[:5]) == b"\x00\x00\x00\x00\x2a"


def test_encode_then_decode_returns_the_same_record(spark):
    avsc = load_avsc("ship.prediction")
    encoded = encode_confluent_avro(_prediction_row(spark), avsc, SCHEMA_ID, key_col="mmsi")

    decoded = decode_confluent_avro(encoded.withColumn("timestamp", F.current_timestamp()), avsc)
    row = decoded.collect()[0]

    assert row.key == "219000001"
    assert [name for name in field_names(avsc) if name not in row.asDict()] == []
    assert row.mmsi == 219000001
    assert row.port == "dkebj"
    assert row.predicted_eta.replace(tzinfo=UTC) == ETA
    assert row.reason_codes == ["dummy_predictor"]
    assert row.lat == 56.15
