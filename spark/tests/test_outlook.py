"""build_outlook counts arrivals and late ships per port and slot."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from common.config import load_thresholds
from streaming.outlook import build_outlook

NOW = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)


def _predictions(spark, rows):
    """rows: (mmsi, predicted_eta offset in minutes from NOW, delay of prediction vs broadcast)"""
    data = [
        ("dkebj", mmsi, NOW + timedelta(minutes=offset - delay), NOW + timedelta(minutes=offset))
        for mmsi, offset, delay in rows
    ]
    return spark.createDataFrame(
        data, "port string, mmsi long, broadcast_eta timestamp, predicted_eta timestamp"
    )


def test_counts_arrivals_and_late_ships_in_one_slot(spark):
    predictions = _predictions(spark, [(1, 70, 0), (2, 100, 45), (3, 110, 0)])

    result = build_outlook(predictions, load_thresholds(), now=NOW).collect()

    assert len(result) == 1
    slot = result[0]
    assert slot.port == "dkebj"
    assert slot.slot_start.replace(tzinfo=UTC) == NOW + timedelta(hours=1)
    assert slot.expected_arrivals == 3
    assert slot.late_count == 1  # 45 minutes late, threshold is 30
    assert slot.bunching_flag is False
    assert slot.day == "2026-10-01"


def test_four_arrivals_in_a_slot_flag_bunching(spark):
    predictions = _predictions(spark, [(i, 65 + i, 0) for i in range(4)])

    slot = build_outlook(predictions, load_thresholds(), now=NOW).collect()[0]

    assert slot.expected_arrivals == 4
    assert slot.bunching_flag is True


def test_ships_in_the_past_or_beyond_the_horizon_are_ignored(spark):
    predictions = _predictions(spark, [(1, -30, 0), (2, 60 * 60, 0), (3, 90, 0)])

    result = build_outlook(predictions, load_thresholds(), now=NOW).collect()

    assert sum(r.expected_arrivals for r in result) == 1
