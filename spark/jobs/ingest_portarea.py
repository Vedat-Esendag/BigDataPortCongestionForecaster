"""R1a: October portarea_<day>.csv.gz caches -> standardized lake zone, rejects to quarantine.

    /lake/standardized/ais_positions/source=portarea/day=<file day>/   Parquet, typed rows
    /lake/quarantine/ais_positions/source=portarea/day=<file day>/     the same, plus reject_reason
    /lake/standardized/_manifests/portarea/<run id>/                   JSON: per-day counts

Only the days listed in year_comparison.json ["days"] are read (optionally only some blocks of
them), and macOS "._*" files are skipped. The source folder is mounted read-only. For each
selected day both output partitions are replaced completely (also when the new result for that
day is empty); days that were not selected keep what they had.

Source order: each gzip file is read alone with textFile (gzip cannot be split, so one
partition) and every raw line is numbered with zipWithIndex before it is parsed. Index 0 is the
header, so line_no = index - 1 is the 0-based data record ordinal that the October pandas code
used (its chunks, cd:1019, and its first-row-wins dedupe, cd:1026 and cd:1031, depend on it).
jobs/check_source_order.py verifies that order against a plain Python read.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import esbjerg_r1 as r1

FILE_RE = re.compile(r"^portarea_(\d{4}-\d{2}-\d{2})\.csv\.gz$")
HEADER = ",".join(r1.CSV_COLUMNS)


def day_files(src: str, days: list[str]) -> dict[str, str]:
    """day -> path for the listed days. "._*" (macOS resource forks) never match."""
    found = {}
    for name in sorted(os.listdir(src)):
        if name.startswith("._"):
            continue
        m = FILE_RE.match(name)
        if m and m.group(1) in days:
            found[m.group(1)] = os.path.join(src, name)
    missing = sorted(set(days) - set(found))
    if missing:
        raise SystemExit(f"listed days without a file in {src}: {missing}")
    return found


def read_day_lines(spark: SparkSession, path: str, day: str) -> DataFrame:
    """(day, idx, value) for every raw line of one file, idx in file order (header = 0)."""
    rdd = spark.sparkContext.textFile("file://" + os.path.abspath(path), 1)
    if rdd.getNumPartitions() != 1:
        raise RuntimeError(f"{path}: expected 1 partition (gzip), got {rdd.getNumPartitions()}")
    indexed = rdd.zipWithIndex().map(lambda t, d=day: (d, t[1], t[0]))
    return spark.createDataFrame(indexed, "day string, idx long, value string")


def check_headers(lines: DataFrame) -> None:
    heads = lines.where("idx = 0").select("day", "value").collect()
    bad = {r["day"]: r["value"] for r in heads if r["value"].lstrip("﻿").strip() != HEADER}
    if bad:
        raise SystemExit(f"unexpected header: {bad}")


def manifest(lines: DataFrame, std: DataFrame, quar: DataFrame) -> dict:
    """Per-day counts. rows_in is counted from the raw lines, independently of the parse."""
    raw = lines.groupBy("day").agg(
        F.sum(F.when((F.col("idx") > 0) & (F.trim("value") != ""), 1).otherwise(0)).alias(
            "rows_in"
        ),
        F.sum(F.when((F.col("idx") > 0) & (F.trim("value") == ""), 1).otherwise(0)).alias(
            "blank_lines"
        ),
    )
    out = std.groupBy("day").agg(
        F.count("*").alias("rows_out"),
        F.sum(F.when(F.col("mobile") == "Class A", 1).otherwise(0)).alias("class_a_rows_out"),
        F.sum(F.when(F.date_format("ts", "yyyy-MM-dd") != F.col("day"), 1).otherwise(0)).alias(
            "ts_date_not_file_day"
        ),
    )
    rej = quar.groupBy("day", "reject_reason").count().collect()
    per_day: dict[str, dict] = {}
    for r in raw.join(out, "day", "left").orderBy("day").collect():
        per_day[r["day"]] = {
            "rows_in": int(r["rows_in"]),
            "blank_lines": int(r["blank_lines"]),
            "rows_out": int(r["rows_out"] or 0),
            "class_a_rows_out": int(r["class_a_rows_out"] or 0),
            "ts_date_not_file_day": int(r["ts_date_not_file_day"] or 0),
            "rejected": {},
        }
    for r in rej:
        per_day[r["day"]]["rejected"][r["reject_reason"]] = int(r["count"])
    totals: dict = {"rows_in": 0, "rows_out": 0, "rejected": 0, "class_a_rows_out": 0}
    reasons: dict[str, int] = {}
    for d in per_day.values():
        d["rejected_total"] = sum(d["rejected"].values())
        d["balanced"] = d["rows_in"] == d["rows_out"] + d["rejected_total"]
        totals["rows_in"] += d["rows_in"]
        totals["rows_out"] += d["rows_out"]
        totals["rejected"] += d["rejected_total"]
        totals["class_a_rows_out"] += d["class_a_rows_out"]
        for k, v in d["rejected"].items():
            reasons[k] = reasons.get(k, 0) + v
    totals["rejected_by_reason"] = reasons
    totals["balanced"] = all(d["balanced"] for d in per_day.values())
    return {"days": sorted(per_day), "per_day": per_day, "totals": totals}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--src", default="/data/portarea", help="folder with portarea_<day>.csv.gz")
    p.add_argument("--days-file", default="/data/portarea/year_comparison.json")
    p.add_argument("--blocks", type=int, nargs="*", help="only these blocks of the day list")
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    args = p.parse_args()

    spark = SparkSession.builder.appName("r1-ingest-portarea").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    t0 = time.time()
    days = r1.select_days(args.days_file, args.blocks)
    files = day_files(args.src, days)
    print(f"[ingest] {len(days)} days {days[0]}..{days[-1]}, blocks {args.blocks or 'all'}")

    parts = [read_day_lines(spark, path, day) for day, path in files.items()]
    lines = parts[0]
    for part in parts[1:]:
        lines = lines.unionByName(part)
    # parsed once, written twice (standardized, quarantine), counted once: keep it, on disk
    lines = lines.persist(StorageLevel.DISK_ONLY)
    check_headers(lines)

    typed = r1.parse_lines(lines)
    std, quar = r1.split_rejects(typed)
    t1 = time.time()
    r1.replace_day_partitions(spark, std, r1.standardized_path(), days)
    print(f"[ingest] standardized written in {time.time() - t1:.1f} s")
    t1 = time.time()
    r1.replace_day_partitions(spark, quar, r1.quarantine_path(), days)
    print(f"[ingest] quarantine written in {time.time() - t1:.1f} s")

    # count from what was written, so the manifest describes the lake, not the plan
    t1 = time.time()
    std_back = r1.read_lake(spark, r1.standardized_path()).where(F.col("day").isin(days))
    quar_back = r1.read_lake(spark, r1.quarantine_path(), r1.QUARANTINE_SCHEMA).where(
        F.col("day").isin(days)
    )
    m = manifest(lines, std_back, quar_back)
    m.update(run_id=args.run_id, src=args.src, blocks=args.blocks, runtime_s=time.time() - t0)
    print(f"[ingest] manifest counted in {time.time() - t1:.1f} s")
    path = r1.lake_path("standardized", f"_manifests/portarea/{args.run_id}")
    spark.createDataFrame([(json.dumps(m),)], "value string").coalesce(1).write.mode(
        "overwrite"
    ).text(path)
    lines.unpersist()

    print("[ingest] day        rows_in   rows_out  rejected  class_a  ts!=day  balanced")
    for d in m["days"]:
        x = m["per_day"][d]
        print(
            f"[ingest] {d} {x['rows_in']:>9} {x['rows_out']:>10} {x['rejected_total']:>9} "
            f"{x['class_a_rows_out']:>8} {x['ts_date_not_file_day']:>8}  {x['balanced']}"
        )
    print(f"[ingest] totals {json.dumps(m['totals'])}")
    print(f"[ingest] manifest {path}")
    print(f"[ingest] runtime {m['runtime_s']:.1f} s")
    spark.stop()
    if not m["totals"]["balanced"]:
        raise SystemExit("rows_in != rows_out + rejected for at least one day")


if __name__ == "__main__":
    main()
