"""Smoke test for a Spark cluster: count the rows of the AIS fixture that ships in the image.

Prints three lines on the driver:
    rows <count>
    master <spark master URL>
    executors <executors registered with the driver>

Needs no other project code, Kafka or HDFS. The CSV is read from the local file system, so the
driver and every executor must run the same image (/opt/app/spark/tests/fixtures/ais_sample.csv).
Run on Kubernetes through infra/k8s/overlays/local (see its README.md).
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

DEFAULT_PATH = "file:///opt/app/spark/tests/fixtures/ais_sample.csv"


def count_rows(spark: SparkSession, path: str) -> int:
    """Rows in the CSV at `path`, not counting the header line."""
    return spark.read.csv(path, header=True).count()


def registered_executors(spark: SparkSession) -> int:
    """Executors currently registered with the driver (the driver itself is not counted)."""
    return spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Count the rows of the AIS fixture CSV")
    parser.add_argument("--path", default=DEFAULT_PATH, help="CSV to count")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("smoke-count").getOrCreate()
    try:
        rows = count_rows(spark, args.path)
        print(f"rows {rows}")
        print(f"master {spark.sparkContext.master}")
        print(f"executors {registered_executors(spark)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
