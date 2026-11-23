"""S2: record a Kafka topic in the raw zone of the data lake, as Parquet on HDFS.

One streaming query per topic:  kafka_to_lake.py --topic ais.position
  - reads the topic from the earliest offset (after the first run, the checkpoint decides)
  - decodes the Confluent-framed Avro with common.avro_io
  - writes every decoded record, unfiltered: bad rows and duplicates that were sent stay in.
    Cleaning is a later step (common/clean.py).
  - output:      lake_path("raw", "<topic with dots as underscores>"), e.g. /lake/raw/ais_position
  - partitions:  day=yyyy-MM-dd, the UTC day of event_time (of the Kafka timestamp when the
                 record could not be decoded and event_time is null)
  - checkpoint:  hdfs://.../checkpoints/kafka_to_lake/<topic>, outside the lake

The file sink and the checkpoint make this exactly-once: a restart continues from the last
committed offsets, and readers only see files listed in the output's _spark_metadata log.
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.avro_io import read_topic
from common.config import lake_path

log = logging.getLogger("kafka_to_lake")

KAFKA_COLUMNS = ("partition", "offset")
METADATA_COLUMNS = ("kafka_partition", "kafka_offset", "kafka_timestamp")
DAY_COLUMN = "day"
SECONDS_PER_DAY = 86400


def utc_day(ts: Column) -> Column:
    """yyyy-MM-dd of a timestamp in UTC, whatever spark.sql.session.timeZone is."""
    days_since_epoch = F.floor(F.unix_seconds(ts) / SECONDS_PER_DAY).cast("int")
    return F.date_from_unix_date(days_since_epoch).cast("string")


def to_lake_rows(decoded: DataFrame, time_column: str = "event_time") -> DataFrame:
    """Decoded Kafka rows -> rows for the raw zone. Keeps every row; only adds the day column.

    Input: key, kafka_timestamp, kafka_partition, kafka_offset and the Avro fields, as returned
    by read_topic(..., kafka_columns=KAFKA_COLUMNS). Output: key, the Avro fields in schema
    order, kafka_partition, kafka_offset, kafka_timestamp, day.
    """
    missing = [
        name for name in ("key", time_column, *METADATA_COLUMNS) if name not in decoded.columns
    ]
    if missing:
        raise ValueError(f"decoded rows lack column(s) {missing}")
    fields = [name for name in decoded.columns if name not in ("key", *METADATA_COLUMNS)]
    day = F.coalesce(utc_day(F.col(time_column)), utc_day(F.col("kafka_timestamp")))
    return decoded.select("key", *fields, *METADATA_COLUMNS, day.alias(DAY_COLUMN))


def lake_dataset(topic: str) -> str:
    return topic.replace(".", "_")


def default_checkpoint(topic: str) -> str:
    base = os.environ.get("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
    return f"{base}/checkpoints/kafka_to_lake/{topic}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Record a Kafka topic in the raw lake zone")
    parser.add_argument("--topic", required=True, help="e.g. ais.position or ais.static")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    parser.add_argument("--checkpoint", help="default: $HDFS_URI/checkpoints/kafka_to_lake/<topic>")
    parser.add_argument(
        "--trigger",
        default="1 minute",
        help='processing interval such as "1 minute" or "30 seconds", or "available-now" to '
        "process what is in the topic and stop",
    )
    parser.add_argument(
        "--time-column", default="event_time", help="timestamp field that sets the day partition"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    output = lake_path("raw", lake_dataset(args.topic))
    checkpoint = args.checkpoint or default_checkpoint(args.topic)

    spark = (
        SparkSession.builder.appName(f"kafka-to-lake-{args.topic}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    decoded = read_topic(
        spark, args.bootstrap, args.topic, starting_offsets="earliest", kafka_columns=KAFKA_COLUMNS
    )
    writer = (
        to_lake_rows(decoded, args.time_column)
        .writeStream.queryName(f"kafka_to_lake_{lake_dataset(args.topic)}")
        .format("parquet")
        .outputMode("append")
        .partitionBy(DAY_COLUMN)
        .option("path", output)
        .option("checkpointLocation", checkpoint)
    )
    if args.trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    query = writer.start()
    log.info(
        "recording %s to %s (checkpoint %s, trigger %s)",
        args.topic,
        output,
        checkpoint,
        args.trigger,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
