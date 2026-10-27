"""Tests for common.clean.

The stub test checks that the function runs and keeps its contract. The strict xfail tests
describe the behaviour F4 must deliver: when cleaning is implemented they start passing, strict
mode then fails the run, and you delete the marker.
"""

from __future__ import annotations

import pytest

from common.clean import clean_positions, clean_static
from common.schemas import REJECT_REASON_COLUMN

NOT_IMPLEMENTED = pytest.mark.xfail(strict=True, reason="TODO(F4): cleaning is not implemented")


def test_sample_fixture_has_the_expected_shape(ais_sample):
    assert ais_sample.count() == 20


def test_clean_positions_stub_runs_and_returns_two_dataframes(ais_sample):
    clean, rejected = clean_positions(ais_sample)

    assert clean.columns == ais_sample.columns
    assert REJECT_REASON_COLUMN in rejected.columns
    assert rejected.count() == 0
    assert clean.count() == 20  # the stub lets every row through


def test_clean_static_stub_runs(spark):
    static = spark.createDataFrame([(219000001, "ESBJERG")], "mmsi long, destination string")

    clean, rejected = clean_static(static)

    assert clean.count() == 1
    assert rejected.count() == 0


@NOT_IMPLEMENTED
def test_duplicates_are_removed(ais_sample):
    clean, _ = clean_positions(ais_sample)
    assert clean.dropDuplicates(["mmsi", "event_time"]).count() == clean.count()


@NOT_IMPLEMENTED
def test_impossible_speed_is_rejected(ais_sample):
    _, rejected = clean_positions(ais_sample)
    assert rejected.where("sog = 85.0").count() == 1


@NOT_IMPLEMENTED
def test_bad_mmsi_is_rejected(ais_sample):
    _, rejected = clean_positions(ais_sample)
    assert sorted(r.mmsi for r in rejected.where("mmsi IN (12345, 0)").collect()) == [0, 12345]


@NOT_IMPLEMENTED
def test_clean_and_rejected_rows_add_up(ais_sample):
    clean, rejected = clean_positions(ais_sample)
    # 20 rows - 1 duplicate = 19 unique; 4 are rejected (speed, 2 MMSI, latitude), 15 remain
    assert clean.count() == 15
    assert rejected.count() == 4
