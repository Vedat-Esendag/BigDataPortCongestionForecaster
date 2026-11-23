"""R1a check: does Spark see every data row of a day file, with every value, in file order?

For each day, hashes (SHA-256) the ordered sequence of all 18 columns of every data row:

    raw     the text fields as read: Python (gzip + csv module, no Spark) vs Spark (textFile +
            zipWithIndex + from_csv, ordered by line_no)
    parsed  the typed values: Python (its own parse: pandas NA strings as null, float(),
            strptime) vs Spark (the ingest's parse_lines) vs the lake (standardized +
            quarantine read back from HDFS), each ordered by line_no

Every side is formatted by the same function: null as \\N, floats with repr(), timestamps as
yyyy-mm-dd HH:MM:SS, fields joined by \\x1f, one row per line. Prints the row counts and hashes;
they must match.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import os
from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import esbjerg_r1 as r1
from jobs.ingest_portarea import day_files, read_day_lines

NULL = "\\N"
SEP = "\x1f"
NA = set(r1.PANDAS_NA_TOKENS)


def canon(value) -> str:
    if value is None:
        return NULL
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S") + (
            f".{value.microsecond:06d}" if value.microsecond else ""
        )
    return str(value)


def digest(rows: Iterable[Iterable]) -> tuple[int, str]:
    h, n = hashlib.sha256(), 0
    for row in rows:
        h.update((SEP.join(canon(v) for v in row) + "\n").encode())
        n += 1
    return n, h.hexdigest()


def py_parse(field: str, column: str):
    """The ingest rules re-implemented in plain Python (cd:1024, pandas defaults)."""
    if field in NA:
        return None
    if column == "ts":
        try:
            return datetime.strptime(field, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if column in r1.NUMERIC_COLUMNS:
        try:
            return float(field)
        except ValueError:
            return None
    return field


def python_rows(path: str, parsed: bool):
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        next(f)  # header
        for line in f:
            if not line.strip():
                continue
            row = next(csv.reader([line]))
            row += [""] * (len(r1.CSV_COLUMNS) - len(row))
            row = row[: len(r1.CSV_COLUMNS)]
            if parsed:
                yield [py_parse(v, c) for v, c in zip(row, r1.CSV_COLUMNS, strict=True)]
            else:
                yield [v if v != "" else None for v in row]  # from_csv reads "" as null


def spark_rows(df):
    cols = list(r1.CSV_COLUMNS)
    for r in df.orderBy("line_no").toLocalIterator():
        yield [r[c] for c in cols]


def main() -> None:
    p = argparse.ArgumentParser(description="SHA-256 of all columns in file order, Spark vs Python")
    p.add_argument("--src", default="/data/portarea")
    p.add_argument("--days", nargs="+", required=True)
    args = p.parse_args()
    spark = SparkSession.builder.appName("r1-check-source-order").getOrCreate()
    files = day_files(args.src, args.days)
    std = r1.read_lake(spark, r1.standardized_path())
    quar = r1.read_lake(spark, r1.quarantine_path(), r1.QUARANTINE_SCHEMA)
    cols = ["line_no", *r1.CSV_COLUMNS]
    ok = True
    for day, path in files.items():
        lines = read_day_lines(spark, path, day)
        raw_sp = lines.where((F.col("idx") > 0) & (F.trim("value") != "")).select(
            (F.col("idx") - 1).alias("line_no"),
            F.from_csv("value", r1.RAW_CSV_DDL, r1.CSV_OPTIONS).alias("r"),
        )
        raw_sp = raw_sp.select("line_no", *[F.col(f"r.{c}").alias(c) for c in r1.CSV_COLUMNS])
        parsed_sp = r1.parse_lines(lines).select(*cols)
        lake = (
            std.where(F.col("day") == day)
            .select(*cols)
            .unionByName(quar.where(F.col("day") == day).select(*cols))
        )
        sides = {
            "raw    python": digest(python_rows(path, parsed=False)),
            "raw    spark ": digest(spark_rows(raw_sp)),
            "parsed python": digest(python_rows(path, parsed=True)),
            "parsed spark ": digest(spark_rows(parsed_sp)),
            "parsed lake  ": digest(spark_rows(lake)),
        }
        raw = {v for k, v in sides.items() if k.startswith("raw")}
        parsed = {v for k, v in sides.items() if k.startswith("parsed")}
        same = len(raw) == 1 and len(parsed) == 1
        ok &= same
        print(f"[order] {day} {os.path.basename(path)}  (18 columns, null as \\N)")
        for name, (n, h) in sides.items():
            print(f"[order]   {name} rows {n:>9}  sha256 {h}")
        print(f"[order]   match {same}")
    spark.stop()
    if not ok:
        raise SystemExit("source order check FAILED")
    print("[order] all days match")


if __name__ == "__main__":
    main()
