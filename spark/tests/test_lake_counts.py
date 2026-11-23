"""jobs/lake_counts.py: the exactly-once check on recorded rows, without HDFS."""

from __future__ import annotations

from jobs.lake_counts import offset_summary, report

SCHEMA = "kafka_partition int, kafka_offset long, mmsi long"


def test_complete_ranges_have_no_duplicates_or_gaps(spark):
    rows = spark.createDataFrame([(0, 0, 1), (0, 1, 1), (0, 2, 2), (1, 0, 3), (1, 1, 3)], SCHEMA)

    summary = offset_summary(rows)

    assert (summary.rows, summary.distinct_offsets, summary.duplicates) == (5, 5, 0)
    assert [(p.partition, p.first, p.last, p.count, p.gaps) for p in summary.partitions] == [
        (0, 0, 2, 3, 0),
        (1, 0, 1, 2, 0),
    ]


def test_a_record_written_twice_is_a_duplicate(spark):
    rows = spark.createDataFrame([(0, 0, 1), (0, 1, 1), (0, 1, 1)], SCHEMA)

    assert offset_summary(rows).duplicates == 1


def test_a_skipped_offset_is_a_gap(spark):
    rows = spark.createDataFrame([(2, 5, 1), (2, 7, 1)], SCHEMA)

    (partition,) = offset_summary(rows).partitions

    assert (partition.first, partition.last, partition.count, partition.gaps) == (5, 7, 2, 1)


def test_report_lines(spark):
    rows = spark.createDataFrame([(0, 0, 1), (0, 1, 1)], SCHEMA)

    assert report("ais_static", offset_summary(rows)) == [
        "ais_static rows 2 distinct_offsets 2 duplicates 0",
        "ais_static partition 0 offsets 0-1 count 2 gaps 0",
    ]
