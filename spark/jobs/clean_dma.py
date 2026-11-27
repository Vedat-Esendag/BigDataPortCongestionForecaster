"""DMA landing CSVs -> Esbjerg rows in the standardized lake zone, bad rows to quarantine.

    in   /lake/landing/dma/day=<day>/aisdk-<day>.csv              scripts/load_dma.py
    out  /lake/standardized/ais_positions/source=dma/day=<day>/    Parquet, STANDARDIZED_SCHEMA
         /lake/quarantine/ais_positions/source=dma/day=<day>/      the same, plus reject_reason
         /lake/standardized/_loads/source=dma/day=<day>/           JSON, one line: the load log

The schema is exactly the one of the October files (common/esbjerg_r1.py), so the arrivals
analysis reads these days unchanged. The rules are in common/dma.py: the port's rows are picked
before anything shuffles. Days are cleaned one at a time; each run replaces the day's three
folders completely, so running a day again never duplicates it.

The load log of a day holds rows in, rows kept, rows quarantined by reason, distinct MMSI,
seconds, and the shuffle Spark wrote for that day (from the driver's status API). Read all days
with spark.read.json("/lake/standardized/_loads/source=dma").

Exit codes: 0 all days cleaned, 3 some days had no landing file (nothing else wrong), 1 failed.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone

from pyspark import StorageLevel
from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F

from common import dma
from common import esbjerg_r1 as r1
from common.config import get_port, lake_path, load_ports

SOURCE = "dma"
EXIT_MISSING = 3


def landing_file(landing: str, day: str) -> str:
    return f"{landing.rstrip('/')}/day={day}/aisdk-{day}.csv"


def load_log_path(day: str) -> str:
    return lake_path("standardized", f"_loads/source={SOURCE}/day={day}")


def _fs(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p, p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())


def file_size(spark: SparkSession, path: str) -> int | None:
    p, fs = _fs(spark, path)
    return int(fs.getFileStatus(p).getLen()) if fs.exists(p) else None


def folder_bytes(spark: SparkSession, path: str) -> int:
    p, fs = _fs(spark, path)
    return int(fs.getContentSummary(p).getLength()) if fs.exists(p) else 0


def check_header(spark: SparkSession, path: str) -> None:
    first = spark.read.text(path).limit(1).first()
    if first is None or first["value"].lstrip("﻿").strip() != dma.DMA_HEADER:
        raise ValueError(f"{path}: not a DMA file (header {first and first['value'][:80]!r})")


def shuffle_metrics(spark: SparkSession, group: str) -> dict | None:
    """Shuffle and input totals of the jobs in `group`, from the driver's status API."""
    url = spark.sparkContext.uiWebUrl
    if not url:
        return None
    base = f"http://localhost:{url.rsplit(':', 1)[1]}/api/v1/applications/"
    base += spark.sparkContext.applicationId

    def get(path: str):
        with urllib.request.urlopen(base + path, timeout=10) as r:
            return json.load(r)

    try:
        for _ in range(20):  # the listener updates the status store a moment after a job ends
            jobs = [j for j in get("/jobs") if j.get("jobGroup") == group]
            if jobs and all(j["status"] != "RUNNING" for j in jobs):
                break
            time.sleep(0.5)
        ids = {s for j in jobs for s in j["stageIds"]}
        stages = [s for s in get("/stages") if s["stageId"] in ids]
    except OSError as exc:
        print(f"[clean-dma] no shuffle metrics: {exc}", flush=True)
        return None
    keys = ("shuffleWriteBytes", "shuffleWriteRecords", "inputBytes", "diskBytesSpilled")
    return {k: sum(int(s.get(k, 0)) for s in stages) for k in keys}


def clean_day(spark: SparkSession, landing: str, day: str, box, run_id: str) -> dict:
    t0 = time.time()
    group = f"clean-dma-{day}-{run_id}"
    spark.sparkContext.setJobGroup(group, f"clean DMA {day}")
    path = landing_file(landing, day)
    check_header(spark, path)

    obs = Observation(f"dma_{day.replace('-', '_')}")
    tagged = dma.tag(dma.read_raw(spark, path, day), box)
    cand = dma.candidates(tagged.observe(obs, *dma.counts(tagged)))
    cand = cand.persist(StorageLevel.MEMORY_AND_DISK)  # read once, used for box and lost rows
    candidates = cand.count()  # the one scan of the day's file; fills the observation
    seen = obs.get

    typed = dma.standardize(dma.port_rows(cand)).persist(StorageLevel.MEMORY_AND_DISK)
    port_rows = typed.count()
    std, quar = r1.split_rejects(typed)
    std_path, quar_path = r1.standardized_path(SOURCE), r1.quarantine_path(SOURCE)
    r1.replace_day_partitions(spark, std, std_path, [day])
    r1.replace_day_partitions(spark, quar, quar_path, [day])
    typed.unpersist()
    cand.unpersist()

    # counted from what was written, so the log describes the lake
    kept = r1.read_lake(spark, std_path).where(F.col("day") == day)
    bad = r1.read_lake(spark, quar_path, r1.QUARANTINE_SCHEMA).where(F.col("day") == day)
    k = kept.agg(
        F.count("*").alias("rows"),
        F.count_distinct("mmsi").alias("mmsi"),
        F.sum(F.when(F.col("mobile") == "Class B", 1).otherwise(0)).alias("class_b"),
    ).first()
    ships = kept.groupBy("mmsi").agg(
        F.count("shiptype").alias("typed"), F.count("name").alias("named")
    )
    s = ships.agg(
        F.sum(F.when(F.col("typed") == 0, 1).otherwise(0)).alias("no_type"),
        F.sum(F.when(F.col("named") == 0, 1).otherwise(0)).alias("no_name"),
    ).first()
    by_reason = {
        r["reason"]: int(r["count"])
        for r in bad.select(F.explode(F.split("reject_reason", ";")).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    }
    n_bad = bad.count()
    seconds = round(time.time() - t0, 1)
    log = {
        "day": day,
        "source": SOURCE,
        "run_id": run_id,
        "landing_file": path,
        "landing_bytes": file_size(spark, path),
        "rows_in": int(seen["rows_in"]),
        "rows_malformed": int(seen["rows_malformed"]),
        "vessel_rows": int(seen["vessel_rows"]),
        "vessel_rows_in_box": int(seen["vessel_rows_in_box"]),
        "vessel_rows_without_position": int(seen["vessel_rows_without_position"]),
        "candidate_rows": int(candidates),
        "port_rows": int(port_rows),
        "rows_kept": int(k["rows"]),
        "rows_kept_class_b": int(k["class_b"] or 0),
        "rows_quarantined": int(n_bad),
        "quarantined_by_reason": {r: by_reason.get(r, 0) for r in dma.REASONS},
        "distinct_mmsi": int(k["mmsi"]),
        "ships_without_type": int(s["no_type"] or 0),
        "ships_without_name": int(s["no_name"] or 0),
        "standardized_bytes": folder_bytes(spark, f"{std_path}/day={day}"),
        "quarantine_bytes": folder_bytes(spark, f"{quar_path}/day={day}"),
        "executors": spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1,
        "seconds": seconds,
        "spark": shuffle_metrics(spark, group),
    }
    # every port row is kept or quarantined, never both or neither
    log["balanced"] = port_rows == log["rows_kept"] + log["rows_quarantined"]
    spark.createDataFrame([(json.dumps(log),)], "value string").coalesce(1).write.mode(
        "overwrite"
    ).text(load_log_path(day))
    spark.sparkContext.setJobGroup("", "")
    return log


def days_between(start: date, end: date) -> list[str]:
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--from", dest="start", type=date.fromisoformat, required=True)
    p.add_argument("--to", dest="end", type=date.fromisoformat, help="inclusive, default --from")
    p.add_argument("--landing", default=lake_path("landing", "dma"))
    p.add_argument("--port", default="dkebj", help="port id in config/ports.yaml")
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    args = p.parse_args()
    end = args.end or args.start
    if end < args.start:
        p.error("--to is before --from")

    spark = SparkSession.builder.appName("clean-dma").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    box = get_port(load_ports(), args.port).bbox
    print(f"[clean-dma] port {args.port} box {box}", flush=True)

    missing, failed = [], []
    for day in days_between(args.start, end):
        if file_size(spark, landing_file(args.landing, day)) is None:
            print(f"[clean-dma] {day} no landing file, skipped", flush=True)
            missing.append(day)
            continue
        try:
            log = clean_day(spark, args.landing, day, box, args.run_id)
        except Exception as exc:  # one bad day must not stop the others
            print(f"[clean-dma] {day} FAILED {exc}", flush=True)
            failed.append(day)
            continue
        sh = log["spark"] or {}
        print(
            f"[clean-dma] {day} in {log['rows_in']:,} kept {log['rows_kept']:,} "
            f"quarantined {log['rows_quarantined']:,} {log['quarantined_by_reason']} "
            f"mmsi {log['distinct_mmsi']} parquet {log['standardized_bytes']:,} B "
            f"shuffle {sh.get('shuffleWriteBytes')} B / {sh.get('shuffleWriteRecords')} rows, "
            f"{log['seconds']} s",
            flush=True,
        )
        print(f"[clean-dma] log {json.dumps(log)}", flush=True)
        if not log["balanced"]:
            failed.append(day)
    spark.stop()
    if failed:
        raise SystemExit(f"failed days: {failed}")
    if missing:
        raise SystemExit(EXIT_MISSING)


if __name__ == "__main__":
    main()
