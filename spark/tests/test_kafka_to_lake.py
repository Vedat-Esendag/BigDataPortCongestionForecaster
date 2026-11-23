"""streaming/kafka_to_lake.py: decoded rows -> raw lake rows, checked without Kafka or HDFS."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.avro_io import decode_confluent_avro, encode_confluent_avro, load_avsc
from streaming.kafka_to_lake import (
    KAFKA_COLUMNS,
    default_checkpoint,
    lake_dataset,
    to_lake_rows,
)

DECODED_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("mmsi", LongType()),
        StructField("event_time", TimestampType()),
        StructField("lat", DoubleType()),
    ]
)


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def _decoded(spark):
    kafka_ts = _utc(2026, 10, 2, 0, 0, 5)
    rows = [
        ("219000001", kafka_ts, 0, 10, 219000001, _utc(2026, 10, 1, 8, 0), 56.15),
        ("219000001", kafka_ts, 0, 11, 219000001, _utc(2026, 10, 1, 8, 0), 56.15),  # duplicate
        ("0", kafka_ts, 1, 3, 0, _utc(2026, 10, 1, 23, 59, 59), 95.0),  # bad MMSI and latitude
        ("12345", kafka_ts, 2, 7, 12345, _utc(2026, 10, 2, 0, 0, 0), 56.1),
        (None, kafka_ts, 2, 8, None, None, None),  # payload that could not be decoded
    ]
    return spark.createDataFrame(rows, DECODED_SCHEMA)


def test_output_columns_and_types(spark):
    result = to_lake_rows(_decoded(spark))

    assert [(f.name, f.dataType.simpleString()) for f in result.schema.fields] == [
        ("key", "string"),
        ("mmsi", "bigint"),
        ("event_time", "timestamp"),
        ("lat", "double"),
        ("kafka_partition", "int"),
        ("kafka_offset", "bigint"),
        ("kafka_timestamp", "timestamp"),
        ("day", "string"),
    ]


def test_every_row_is_kept_including_duplicates_and_bad_rows(spark):
    result = to_lake_rows(_decoded(spark))

    assert result.count() == 5
    assert result.filter(F.col("kafka_offset").isin(10, 11)).count() == 2


def test_day_is_the_utc_day_of_event_time(spark):
    days = {row.kafka_offset: row.day for row in to_lake_rows(_decoded(spark)).collect()}

    assert days[10] == "2026-10-01"
    assert days[3] == "2026-10-01"  # 23:59:59 UTC is still the same day
    assert days[7] == "2026-10-02"  # midnight UTC starts the next day


def test_day_is_utc_even_when_the_session_time_zone_is_not(spark):
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        days = {row.kafka_offset: row.day for row in to_lake_rows(_decoded(spark)).collect()}
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    assert days[3] == "2026-10-01"
    assert days[7] == "2026-10-02"


def test_undecodable_record_takes_its_day_from_the_kafka_timestamp(spark):
    row = to_lake_rows(_decoded(spark)).filter(F.col("kafka_offset") == 8).collect()[0]

    assert row.event_time is None
    assert row.day == "2026-10-02"


def test_missing_time_column_is_rejected(spark):
    with pytest.raises(ValueError, match="event_time"):
        to_lake_rows(_decoded(spark).drop("event_time"))


def test_decode_keeps_kafka_partition_and_offset_when_asked(spark):
    avsc = load_avsc("ais.position")
    record = spark.createDataFrame(
        [(219000001, _utc(2026, 10, 1, 8, 0), _utc(2026, 10, 1, 8, 0), 56.15, 10.25, "fixture")],
        "mmsi long, event_time timestamp, received_time timestamp, lat double, lon double, "
        "source string",
    ).select(
        "mmsi",
        "event_time",
        "received_time",
        "lat",
        "lon",
        *[
            F.lit(None).cast(t).alias(n)
            for n, t in [
                ("sog", "double"),
                ("cog", "double"),
                ("heading", "int"),
                ("rot", "double"),
                ("nav_status", "int"),
                ("message_type", "int"),
            ]
        ],
        "source",
    )
    kafka = (
        encode_confluent_avro(record, avsc, 42, key_col="mmsi")
        .withColumn("timestamp", F.current_timestamp())
        .withColumn("partition", F.lit(2))
        .withColumn("offset", F.lit(17).cast("long"))
    )

    decoded = decode_confluent_avro(kafka, avsc, kafka_columns=KAFKA_COLUMNS)
    row = to_lake_rows(decoded).collect()[0]

    assert decoded.columns[:4] == ["key", "kafka_timestamp", "kafka_partition", "kafka_offset"]
    assert (row.kafka_partition, row.kafka_offset, row.mmsi, row.day) == (
        2,
        17,
        219000001,
        "2026-10-01",
    )


def test_paths_follow_the_topic_name():
    assert lake_dataset("ais.history.position") == "ais_history_position"
    assert default_checkpoint("ais.position").endswith("/checkpoints/kafka_to_lake/ais.position")
