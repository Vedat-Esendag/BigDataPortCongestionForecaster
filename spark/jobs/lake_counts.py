"""Check what the recorder (streaming/kafka_to_lake.py) wrote to the raw zone.

For each dataset, prints on the driver:
    <dataset> rows <n> distinct_offsets <n> duplicates <n>
    <dataset> partition <p> offsets <min>-<max> count <n> gaps <n>

"duplicates 0" and "gaps 0" mean every Kafka record was written exactly once, which is what the
restart test checks. Runs inside the cluster (HDFS cannot be reached from a laptop), as the
lake-check Job in infra/k8s/spark/school/one-off.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import lake_path

DEFAULT_DATASETS = ("ais_position", "ais_static")


@dataclass(frozen=True)
class PartitionOffsets:
    partition: int
    first: int
    last: int
    count: int

    @property
    def gaps(self) -> int:
        """Offsets between first and last that are missing (0 when the range is complete)."""
        return self.last - self.first + 1 - self.count


@dataclass(frozen=True)
class OffsetSummary:
    rows: int
    distinct_offsets: int
    partitions: tuple[PartitionOffsets, ...]

    @property
    def duplicates(self) -> int:
        return self.rows - self.distinct_offsets


def offset_summary(rows: DataFrame) -> OffsetSummary:
    """Rows, distinct (kafka_partition, kafka_offset) pairs and the offset range per partition."""
    offsets = rows.select("kafka_partition", "kafka_offset")
    per_partition = (
        offsets.distinct()
        .groupBy("kafka_partition")
        .agg(
            F.min("kafka_offset").alias("first"),
            F.max("kafka_offset").alias("last"),
            F.count("*").alias("count"),
        )
        .orderBy("kafka_partition")
        .collect()
    )
    partitions = tuple(
        PartitionOffsets(r.kafka_partition, r.first, r.last, r["count"]) for r in per_partition
    )
    return OffsetSummary(
        rows=offsets.count(),
        distinct_offsets=sum(p.count for p in partitions),
        partitions=partitions,
    )


def report(dataset: str, summary: OffsetSummary) -> list[str]:
    lines = [
        f"{dataset} rows {summary.rows} distinct_offsets {summary.distinct_offsets} "
        f"duplicates {summary.duplicates}"
    ]
    lines += [
        f"{dataset} partition {p.partition} offsets {p.first}-{p.last} count {p.count} "
        f"gaps {p.gaps}"
        for p in summary.partitions
    ]
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Count the raw lake rows per Kafka offset")
    parser.add_argument(
        "--dataset",
        action="append",
        help=f"raw zone dataset, repeatable (default: {', '.join(DEFAULT_DATASETS)})",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lake-check").getOrCreate()
    try:
        for dataset in args.dataset or DEFAULT_DATASETS:
            path = lake_path("raw", dataset)
            try:
                rows = spark.read.parquet(path)
            except AnalysisException as exc:
                # Spark cannot read a folder without data files: before the first batch with
                # rows, or when the recorder never ran. A result, not a crash.
                print(f"{dataset} no data files yet ({path}): {type(exc).__name__}")
                continue
            for line in report(dataset, offset_summary(rows)):
                print(line)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
