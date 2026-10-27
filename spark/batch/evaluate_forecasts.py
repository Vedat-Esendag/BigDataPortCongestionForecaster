"""F12: evaluate past predictions against what actually happened.

Compares `ship.prediction` history with the real arrival times and reports, per port, model
version and horizon: error of the predicted ETA, error of the broadcast ETA (the baseline we have
to beat), and how often the real arrival fell inside the 70% window. Results go to the
`eval.metrics` Kafka topic and the Cassandra sink connector stores them.

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

TOPIC = "eval.metrics"
log = logging.getLogger("evaluate_forecasts")

RESULT_SCHEMA = StructType(
    [
        StructField("port", StringType(), False),
        StructField("day", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("horizon_hours", IntegerType(), False),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("n_samples", IntegerType(), False),
        StructField("computed_at", TimestampType(), False),
    ]
)


def evaluate(spark: SparkSession, day: str) -> DataFrame:
    """Score the predictions made on `day` (yyyy-MM-dd, UTC).

    TODO(F12): read ship.prediction history for the day from the lake, join the actual arrival
               time from port events, and compute mae_minutes, broadcast_mae_minutes,
               coverage_70 and n_samples per (port, model_version, horizon_hours).
    """
    return spark.createDataFrame([], RESULT_SCHEMA)


def main() -> None:
    parser = argparse.ArgumentParser(description="Forecast evaluation")
    parser.add_argument("--day", required=True, help="UTC day to evaluate, yyyy-MM-dd")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    parser.add_argument(
        "--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("evaluate-forecasts").getOrCreate()
    result = evaluate(spark, args.day)
    if result.isEmpty():
        log.warning("no result rows (stub), nothing written to %s", TOPIC)
        return
    write_topic_batch(
        result, args.bootstrap, TOPIC, fetch_schema_id(args.registry, TOPIC), key_col="port"
    )


if __name__ == "__main__":
    main()
