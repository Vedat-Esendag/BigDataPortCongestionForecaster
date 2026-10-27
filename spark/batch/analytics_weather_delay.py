"""F5: weather-to-delay analytics.

Question answered: how much later than the broadcast ETA do ships arrive when wind or waves are
high? The result goes to the `analytics.weather_delay` Kafka topic and the Cassandra sink
connector stores it, the same path the live results take.

STUB: computes nothing and writes nothing.
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.avro_io import fetch_schema_id, write_topic_batch

TOPIC = "analytics.weather_delay"
log = logging.getLogger("analytics_weather_delay")

RESULT_SCHEMA = StructType(
    [
        StructField("port", StringType(), False),
        StructField("weather_variable", StringType(), False),
        StructField("bucket", StringType(), False),
        StructField("n_arrivals", IntegerType(), False),
        StructField("mean_delay_min", DoubleType(), False),
        StructField("median_delay_min", DoubleType(), False),
        StructField("p90_delay_min", DoubleType(), False),
        StructField("period_start", TimestampType(), False),
        StructField("period_end", TimestampType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def compute_weather_delay(spark: SparkSession) -> DataFrame:
    """Join historical arrivals with weather and summarise delay per weather bucket.

    TODO(F5): read arrivals (port events from common.port_events) and the broadcast ETA from the
              `standardized` zone, read DMI observations and Open-Meteo archive data, compute
              delay = actual arrival minus broadcast ETA, bucket wind speed and wave height, and
              aggregate mean, median and p90 delay per (port, variable, bucket).
    """
    return spark.createDataFrame([], RESULT_SCHEMA)


def main() -> None:
    parser = argparse.ArgumentParser(description="Weather-to-delay analytics")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    parser.add_argument(
        "--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("analytics-weather-delay").getOrCreate()
    result = compute_weather_delay(spark)
    if result.isEmpty():
        log.warning("no result rows (stub), nothing written to %s", TOPIC)
        return
    write_topic_batch(
        result, args.bootstrap, TOPIC, fetch_schema_id(args.registry, TOPIC), key_col="port"
    )


if __name__ == "__main__":
    main()
