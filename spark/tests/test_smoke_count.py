"""jobs/smoke_count.py counts the fixture rows the same way it does on the cluster."""

from __future__ import annotations

from pathlib import Path

from jobs.smoke_count import count_rows

FIXTURE = Path(__file__).parent / "fixtures" / "ais_sample.csv"


def test_count_rows_counts_the_fixture_without_its_header(spark):
    assert count_rows(spark, str(FIXTURE)) == 20
