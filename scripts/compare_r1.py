"""R1c: compare the Spark Esbjerg tables with the October pandas output, event by event.

Runs with spark-submit in the compose Spark container (the image has no pandas), with
spark/dist/common.zip on --py-files. Reads the Spark tables from /lake/curated and the October
CSVs from the read-only mount (/data/portarea/year/esbjerg_{arrivals,stops,eta}.csv).

    arrivals  key (block, mmsi, arrival)  fields stops, anchor_stops, wait_min, anchor_wait_min, ...
    stops     key (block, mmsi, start)    fields end, minutes, anchored_share, lat, lon, ...
    ETA       key (block, mmsi, arrival)  fields ts, eta

For each: matched / only-Spark / only-October, then per-field differences among the matched
rows. Joins are null-safe (<=>) and the keys are checked to be unique on both sides first.
Then the summary numbers side by side, the Nordby count, and the first mismatches with both rows
and a preview of the ship's track. --inspect BLOCK:MMSI prints the full stop runs of one ship.
Neither side is assumed correct; the script shows evidence, it does not pick a winner.
Exit status: 0 only when every event, field and summary comparison (and, on the full 84-day run,
the year_comparison.json check) matches; 1 otherwise, after all diagnostics are printed.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import timedelta
from itertools import pairwise

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import esbjerg_r1 as r1

TOL = 1e-6
ARRIVAL_FIELDS = {
    "name": "string",
    "type": "string",
    "length": "double",
    "track_start": "timestamp",
    "stops": "int",
    "anchor_stops": "int",
    "wait_min": "double",
    "anchor_wait_min": "double",
    "moored_stops_elsewhere": "int",
    "large": "boolean",
}
STOP_FIELDS = {
    "end": "timestamp",
    "minutes": "double",
    "anchored_share": "double",
    "lat": "double",
    "lon": "double",
    "arrival": "timestamp",
    "cut_at_start": "boolean",
    "anchored": "boolean",
    "type": "string",
    "length": "double",
}
ETA_FIELDS = {"ts": "timestamp", "eta": "timestamp"}


# ---------------------------------------------------------------- loading
def read_october(spark: SparkSession, path: str, cols: dict[str, str]) -> DataFrame:
    df = spark.read.csv(path, header=True, quote='"', escape='"', inferSchema=False)
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in cols.items()])


def load(spark: SparkSession, oct_dir: str, blocks: list[int] | None):
    a_cols = {"block": "int", "mmsi": "string", "arrival": "timestamp", **ARRIVAL_FIELDS}
    s_cols = {"block": "int", "mmsi": "string", "start": "timestamp", **STOP_FIELDS}
    e_cols = {"block": "int", "mmsi": "string", "arrival": "timestamp", **ETA_FIELDS}
    o_arr = read_october(spark, f"{oct_dir}/esbjerg_arrivals.csv", a_cols)
    o_stp = read_october(spark, f"{oct_dir}/esbjerg_stops.csv", s_cols)
    o_eta = read_october(spark, f"{oct_dir}/esbjerg_eta.csv", e_cols)
    s_arr = spark.read.parquet(r1.curated_path("esbjerg_arrivals"))
    s_stp = spark.read.parquet(r1.curated_path("esbjerg_stops"))
    s_eta = spark.read.parquet(r1.curated_path("esbjerg_eta_at_entry"))
    if blocks is not None:
        o_arr, o_stp, o_eta = (d.where(F.col("block").isin(blocks)) for d in (o_arr, o_stp, o_eta))
    return (s_arr, s_stp, s_eta), (o_arr, o_stp, o_eta)


def assert_unique(df: DataFrame, keys: list[str], label: str) -> None:
    n, k = df.count(), df.select(*keys).distinct().count()
    if n != k:
        raise SystemExit(f"{label}: key {keys} is not unique ({n} rows, {k} distinct keys)")


# ---------------------------------------------------------------- event comparison
def compare(s: DataFrame, o: DataFrame, keys: list[str], fields: dict[str, str], label: str):
    assert_unique(s, keys, f"Spark {label}")
    assert_unique(o, keys, f"October {label}")
    sl = s.select(*keys, *[F.col(c).alias(f"s_{c}") for c in fields], F.lit(True).alias("s_in"))
    ol = o.select(
        *[F.col(k).alias(f"o_k_{k}") for k in keys],
        *[F.col(c).alias(f"o_{c}") for c in fields],
        F.lit(True).alias("o_in"),
    )
    cond = None
    for k in keys:
        c = F.col(k).eqNullSafe(F.col(f"o_k_{k}"))
        cond = c if cond is None else cond & c
    j = sl.join(ol, cond, "full_outer")
    for k in keys:
        j = j.withColumn(k, F.coalesce(F.col(k), F.col(f"o_k_{k}"))).drop(f"o_k_{k}")
    status = (
        F.when(F.col("s_in") & F.col("o_in"), "matched")
        .when(F.col("s_in"), "only_spark")
        .otherwise("only_october")
    )
    j = j.withColumn("status", status)
    diffs = []
    for c, t in fields.items():
        sc, oc = F.col(f"s_{c}"), F.col(f"o_{c}")
        if t == "double":
            eq = (sc.isNull() & oc.isNull()) | (F.abs(sc - oc) <= TOL)
        else:
            eq = sc.eqNullSafe(oc)
        j = j.withColumn(f"eq_{c}", F.coalesce(eq, F.lit(False)))
        diffs.append(F.when(~F.col(f"eq_{c}"), F.lit(c)))
    j = j.withColumn("diff_fields", F.array_compact(F.array(*diffs))).persist()

    counts = {r["status"]: r["count"] for r in j.groupBy("status").count().collect()}
    print(f"\n== {label}: key {tuple(keys)}")
    print(
        f"   matched {counts.get('matched', 0)}   only Spark {counts.get('only_spark', 0)}   "
        f"only October {counts.get('only_october', 0)}"
    )
    matched = j.where("status = 'matched'")
    agg = matched.agg(*[F.sum(F.when(~F.col(f"eq_{c}"), 1).otherwise(0)).alias(c) for c in fields])
    row = agg.first()
    print("   field differences among matched: " + ", ".join(f"{c} {row[c] or 0}" for c in fields))
    return j, counts, {c: int(row[c] or 0) for c in fields}


# ---------------------------------------------------------------- summary numbers
def summary(arr: list[dict], stp: list[dict], eta: list[dict], ndays: int) -> dict:
    """year_analysis.summarise numbers that need no wind (ya:81-121), plus ETA (mf:254-262)."""
    large = [a for a in arr if a["large"]]
    w = [a for a in large if a["anchor_stops"] > 0]
    wait_h = [a["anchor_wait_min"] / 60 for a in w]
    waiter_mmsi = {a["mmsi"] for a in w}  # ya:94 isin on mmsi only, not (block, mmsi)
    ast = [s for s in stp if s["anchored"] and s["mmsi"] in waiter_mmsi]
    per_month: dict[str, int] = {}
    for a in w:
        m = a["arrival"].strftime("%Y-%m")
        per_month[m] = per_month.get(m, 0) + 1
    out = {
        "arrivals_all": len(arr),
        "arrivals_large": len(large),
        "large_anchor_waits": len(w),
        "wait_rate_large_pct": 100 * len(w) / len(large) if large else None,
        "all_anchor_waits": sum(1 for a in arr if a["anchor_stops"] > 0),
        "est_waits_per_year": len(w) * 365 / ndays,
        "wait_median_h": statistics.median(wait_h) if wait_h else None,
        "wait_max_h": max(wait_h) if wait_h else None,
        "waits_12h_plus": sum(1 for h in wait_h if h >= 12),
        "censored_waits_cut_at_start": sum(1 for s in ast if s["cut_at_start"]),
        "moored_stops_elsewhere": sum(a["moored_stops_elsewhere"] for a in arr),
        "stops": len(stp),
        "anchored_stops": sum(1 for s in stp if s["anchored"]),
        "waits_per_month": dict(sorted(per_month.items())),
    }
    has = [e for e in eta if e["eta"] is not None]
    diff = [(e["arrival"] - e["eta"]).total_seconds() / 3600 for e in has]
    keep = [d for d in diff if abs(d) <= r1.ETA_MAX_ABS_H]
    out.update(
        eta_large_arrivals=len(eta),
        eta_with=len(has),
        eta_dropped_over_7d=len(has) - len(keep),
        eta_kept=len(keep),
        eta_median_h=statistics.median(keep) if keep else None,
        eta_pct_within_2h=100 * sum(1 for d in keep if abs(d) <= 2) / len(keep) if keep else None,
        eta_median_abs_h=statistics.median(abs(d) for d in keep) if keep else None,
        eta_max_abs_h=max(abs(d) for d in keep) if keep else None,
    )
    return out


def fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


def same_value(a, b) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(a - b) <= TOL
    return a == b


def print_side_by_side(s: dict, o: dict, title: str) -> list[str]:
    """Prints the metrics side by side; returns the names of the metrics that differ."""
    bad = []
    print(f"\n== {title}")
    print(f"   {'metric':<30}{'Spark':>22}{'October':>22}{'diff':>12}")
    for k in s:
        sv, ov = s[k], o.get(k)
        if isinstance(sv, dict):
            months = sorted(set(sv) | set(ov or {}))
            for mth in months:
                a, b = sv.get(mth, 0), (ov or {}).get(mth, 0)
                print(f"   {k + ' ' + mth:<30}{a:>22}{b:>22}{a - b:>12}")
                if a != b:
                    bad.append(f"{k} {mth}")
            continue
        d = sv - ov if isinstance(sv, (int, float)) and isinstance(ov, (int, float)) else None
        print(f"   {k:<30}{fmt(sv):>22}{fmt(ov):>22}{fmt(d):>12}")
        if not same_value(sv, ov):
            bad.append(k)
    return bad


# ---------------------------------------------------------------- per-ship evidence
def annotated(spark: SparkSession, days: list[str], mmsis: set[str]) -> DataFrame:
    std = r1.read_lake(spark, r1.standardized_path()).where(F.col("day").isin(days))
    day_source = std.where((F.col("mobile") == "Class A") & F.col("mmsi").isNotNull())
    a = r1.class_a_thinned(std.where(F.col("mmsi").isin(list(mmsis))), days)
    return r1.annotate_tracks(r1.flag_rows(r1.add_blocks(a, day_source.select("ts"))))


TRACK_COLS = [
    "rn",
    "ts",
    "lat",
    "lon",
    "sog",
    "nav",
    "in_zone",
    "ign",
    "wait_flag",
    "brk",
    "run_id",
]


def show_rows(rows: list) -> None:
    for r in rows:
        print("      " + "  ".join(f"{c}={fmt(r[c])}" for c in TRACK_COLS))


def print_mismatches(spark, days, joined: list[tuple[str, DataFrame, dict]], n: int) -> None:
    picked = []
    for label, j, fields in joined:
        bad = j.where("status != 'matched' OR size(diff_fields) > 0")
        order = [F.col("block"), F.col("mmsi")] + [
            F.col(c) for c in ("arrival", "start") if c in j.columns
        ]
        for r in bad.orderBy(*order).limit(n).collect():
            picked.append((label, r, fields))
    picked = picked[:n]
    if not picked:
        print("\n== no mismatches")
        return
    tracks = annotated(spark, days, {r["mmsi"] for _, r, _ in picked}).persist()
    print(f"\n== first {len(picked)} mismatches (evidence; the judgement is in the R1 report)")
    for i, (label, r, fields) in enumerate(picked, 1):
        d = r.asDict()
        ref = d.get("arrival") or d.get("s_arrival") or d.get("o_arrival")
        print(
            f"\n-- {i}. {label} {r['status']} block {r['block']} mmsi {r['mmsi']} "
            f"diff {list(r['diff_fields']) if r['status'] == 'matched' else '-'}"
        )
        keys = [k for k in ("arrival", "start") if k in r.asDict()]
        print("   key   " + "  ".join(f"{k}={fmt(r[k])}" for k in keys))
        print("   Spark   " + "  ".join(f"{c}={fmt(r['s_' + c])}" for c in fields))
        print("   October " + "  ".join(f"{c}={fmt(r['o_' + c])}" for c in fields))
        g = tracks.where((F.col("block") == r["block"]) & (F.col("mmsi") == r["mmsi"]))
        if ref is not None:
            g = g.where(F.col("ts").between(ref - timedelta(hours=6), ref))
        print("   track preview (6 h up to the arrival, at most 40 rows):")
        show_rows(g.orderBy(F.col("ts").desc()).limit(40).orderBy("ts").collect())
    tracks.unpersist()


def inspect(spark, days, target: str) -> None:
    """Every run before the first entry, and the rows around each wait run and the entry."""
    block, mmsi = target.split(":")
    g = annotated(spark, days, {mmsi}).where(
        (F.col("block") == int(block)) & (F.col("mmsi") == mmsi)
    )
    rows = g.orderBy("rn").collect()
    if not rows:
        print(f"\n== inspect {target}: no rows")
        return
    first = rows[0]["first_rn"]
    print(f"\n== inspect block {block} mmsi {mmsi}: {len(rows)} rows, first entry rn {first}")
    runs: dict[int, list] = {}
    for r in rows:
        if first is None or r["rn"] <= first:
            runs.setdefault(r["run_id"], []).append(r)
    print(
        "   runs up to the entry "
        "(run_id, flag, rows, start, end, minutes, anchored, moored, max gap min):"
    )
    for rid, rr in runs.items():
        mins = (rr[-1]["ts"] - rr[0]["ts"]).total_seconds() / 60
        gaps = [(b["ts"] - a["ts"]).total_seconds() / 60 for a, b in pairwise(rr)]
        print(
            f"      {rid:>4} {rr[0]['wait_flag']!s:<5} {len(rr):>5} {rr[0]['ts']} {rr[-1]['ts']} "
            f"{mins:8.1f} {sum(x['anchored'] for x in rr) / len(rr):.3f} "
            f"{sum(x['moored'] for x in rr) / len(rr):.3f} {max(gaps, default=0):6.1f}"
        )
    by_rn = {r["rn"]: r for r in rows}
    for rid, rr in runs.items():
        if not rr[0]["wait_flag"] or (rr[-1]["ts"] - rr[0]["ts"]).total_seconds() < 25 * 60:
            continue
        lo, hi = rr[0]["rn"], rr[-1]["rn"]
        print(f"   wait run {rid}: rows {lo}-{hi}, two before, first/last two, two after")
        pick = [x for x in (lo - 2, lo - 1, lo, lo + 1, hi - 1, hi, hi + 1, hi + 2) if x in by_rn]
        show_rows([by_rn[x] for x in sorted(set(pick))])
    if first is not None:
        print("   entry: three rows before and after")
        show_rows([by_rn[x] for x in range(first - 3, first + 4) if x in by_rn])


# ---------------------------------------------------------------- main
def main() -> None:
    p = argparse.ArgumentParser(description="Compare the Spark R1 tables with the October output")
    p.add_argument("--oct-dir", default="/data/portarea/year")
    p.add_argument("--days-file", default="/data/portarea/year_comparison.json")
    p.add_argument("--blocks", type=int, nargs="*", help="only these blocks (as the Spark run)")
    p.add_argument("--mismatches", type=int, default=5)
    p.add_argument("--inspect", action="append", default=[], metavar="BLOCK:MMSI")
    args = p.parse_args()

    spark = SparkSession.builder.appName("r1-compare").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    days = r1.select_days(args.days_file, args.blocks)
    (s_arr, s_stp, s_eta), (o_arr, o_stp, o_eta) = load(spark, args.oct_dir, args.blocks)
    print(f"R1 comparison, {len(days)} days {days[0]}..{days[-1]}, blocks {args.blocks or 'all'}")

    failures: list[str] = []
    joined = {}
    for label, s_df, o_df, keys, fields in (
        ("arrivals", s_arr, o_arr, ["block", "mmsi", "arrival"], ARRIVAL_FIELDS),
        ("stops", s_stp, o_stp, ["block", "mmsi", "start"], STOP_FIELDS),
        ("ETA reported at entry", s_eta, o_eta, ["block", "mmsi", "arrival"], ETA_FIELDS),
    ):
        j, counts, diffs = compare(s_df, o_df, keys, fields, label)
        joined[label] = j
        for status in ("only_spark", "only_october"):
            if counts.get(status, 0):
                failures.append(f"{label}: {counts[status]} {status.replace('_', ' ')}")
        failures += [f"{label}: field {c} differs in {n}" for c, n in diffs.items() if n]
    ja, js, je = joined["arrivals"], joined["stops"], joined["ETA reported at entry"]

    def rows(df):
        return [r.asDict() for r in df.collect()]

    s_sum = summary(rows(s_arr), rows(s_stp), rows(s_eta), len(days))
    o_sum = summary(rows(o_arr), rows(o_stp), rows(o_eta), len(days))
    bad = print_side_by_side(s_sum, o_sum, "summary (from each side's tables)")
    failures += [f"summary: {k} differs" for k in bad]

    nb = s_arr.agg(
        F.sum(F.col("entry_in_nordby").cast("int")).alias("n"),
        F.sum((F.col("entry_in_nordby") & F.col("large")).cast("int")).alias("large"),
    ).first()
    print(
        f"\n== Nordby: Spark arrivals whose entry row lies inside NORDBY_ZONE: {nb['n']}, "
        f"of them large: {nb['large']} (counted as entering Esbjerg, as in October)"
    )

    full = args.blocks is None and os.path.exists(args.days_file)
    if full:
        with open(args.days_file) as f:
            port = json.load(f)["ports"]["esbjerg"]
        ref = {k: port[k] for k in s_sum if k in port}
        if "waits_per_month_in_sample" in port:
            ref["waits_per_month"] = port["waits_per_month_in_sample"]
        bad = print_side_by_side(
            {k: s_sum[k] for k in ref}, ref, "Spark vs year_comparison.json (esbjerg)"
        )
        failures += [f"year_comparison.json: {k} differs" for k in bad]

    print_mismatches(
        spark,
        days,
        [("arrival", ja, ARRIVAL_FIELDS), ("stop", js, STOP_FIELDS), ("eta", je, ETA_FIELDS)],
        args.mismatches,
    )
    for target in args.inspect:
        inspect(spark, days, target)
    spark.stop()
    if failures:
        print(f"\n== RESULT: FAIL ({len(failures)} checks)")
        for f in failures:
            print(f"   {f}")
        sys.exit(1)
    print("\n== RESULT: PASS (every event, field and summary number matches)")


if __name__ == "__main__":
    main()
