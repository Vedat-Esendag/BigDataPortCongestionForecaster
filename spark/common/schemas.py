"""Spark schemas shared across jobs. Field names match schemas/avro/*.avsc."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

POSITION_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("received_time", TimestampType(), True),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("sog", DoubleType(), True),
        StructField("cog", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("rot", DoubleType(), True),
        StructField("nav_status", IntegerType(), True),
        StructField("message_type", IntegerType(), True),
        StructField("source", StringType(), True),
    ]
)

# Columns of the small CSV used in tests and in notebooks (a subset of POSITION_SCHEMA).
POSITION_CSV_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("sog", DoubleType(), True),
        StructField("cog", DoubleType(), True),
        StructField("heading", IntegerType(), True),
    ]
)

STATIC_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("received_time", TimestampType(), True),
        StructField("imo", LongType(), True),
        StructField("call_sign", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ship_type", IntegerType(), True),
        StructField("length_m", DoubleType(), True),
        StructField("beam_m", DoubleType(), True),
        StructField("draught_m", DoubleType(), True),
        StructField("destination", StringType(), True),
        StructField("eta_broadcast", TimestampType(), True),
        StructField("source", StringType(), True),
    ]
)

# One row per detected port event (entered approach zone, anchored, berthed, left).
PORT_EVENT_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), False),
        StructField("port", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", TimestampType(), False),
    ]
)

# Rows removed by cleaning keep their data and gain the reason they were rejected.
REJECT_REASON_COLUMN = "reject_reason"

# One row per ship ready for the predictor. TODO(F6): grow this as features are added.
FEATURE_BASE_SCHEMA = StructType(
    [
        StructField("mmsi", LongType(), False),
        StructField("port", StringType(), True),
        StructField("broadcast_eta", TimestampType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]
)

OUTLOOK_FIELDS = (
    "port",
    "slot_start",
    "slot_end",
    "expected_arrivals",
    "late_count",
    "bunching_flag",
    "expected_waiting_ships",
    "computed_at",
    "day",
)

__all__ = [
    "FEATURE_BASE_SCHEMA",
    "OUTLOOK_FIELDS",
    "PORT_EVENT_SCHEMA",
    "POSITION_CSV_SCHEMA",
    "POSITION_SCHEMA",
    "REJECT_REASON_COLUMN",
    "STATIC_SCHEMA",
]
