"""R1b: Esbjerg arrivals, stops and "ETA reported at entry", with the October method in Spark.

Reads /lake/standardized/ais_positions/source=portarea (and its quarantine, for ETA only) and
writes Parquet:

    /lake/curated/esbjerg_arrivals         one row per (block, mmsi) that enters the harbour zone
    /lake/curated/esbjerg_stops            the counted stops before each first entry
    /lake/curated/esbjerg_eta_at_entry     every large arrival with the last ETA it sent (3 days)
    /lake/curated/_manifests/esbjerg_r1/<run id>/   JSON: row counts per stage, runtimes

The rules and their October line numbers are in common/esbjerg_r1.py. Wind is out of scope.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import esbjerg_r1 as r1


class Stages:
    def __init__(self) -> None:
        self.t0 = time.time()
        self.last = self.t0
        self.counts: dict[str, int] = {}
        self.seconds: dict[str, float] = {}

    def done(self, name: str, count: int | None = None) -> None:
        now = time.time()
        self.seconds[name] = round(now - self.last, 1)
        self.last = now
        if count is not None:
            self.counts[name] = int(count)
        shown = "" if count is None else f"{count:>12,}"
        print(f"[r1] {name:<34}{shown}  {self.seconds[name]:>7.1f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--days-file", default="/data/portarea/year_comparison.json")
    p.add_argument("--blocks", type=int, nargs="*", help="only these blocks of the day list")
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    args = p.parse_args()

    spark = SparkSession.builder.appName("r1-esbjerg-arrivals").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    st = Stages()
    days = r1.select_days(args.days_file, args.blocks)
    print(f"[r1] {len(days)} days {days[0]}..{days[-1]}, blocks {args.blocks or 'all'}")

    std = r1.read_lake(spark, r1.standardized_path()).where(F.col("day").isin(days))
    quar = r1.read_lake(spark, r1.quarantine_path(), r1.QUARANTINE_SCHEMA).where(
        F.col("day").isin(days)
    )
    st.done("standardized rows (listed days)", std.count())
    st.done("quarantined rows (listed days)", quar.count())
    st.done(
        "class A rows",
        std.where((F.col("mobile") == "Class A") & F.col("mmsi").isNotNull()).count(),
    )
    ties = r1.tie_counts(std, days)
    st.done("(mmsi, ts) duplicate rows", ties["tie_rows"])
    st.counts.update(ties)
    print(
        f"[r1]   tie groups {ties['tie_groups']}, disagreeing on nav/sog "
        f"{ties['tie_groups_conflicting']} (the first row in file order wins)"
    )

    a = r1.class_a_thinned(std, days).persist(StorageLevel.MEMORY_AND_DISK)
    st.done("after minute thinning and dedupe", a.count())

    flagged = r1.flag_rows(r1.add_blocks(a))
    groups = r1.annotate_tracks(flagged).groupBy("block", "mmsi").agg(F.min("first_rn").alias("f"))
    g = groups.agg(
        F.count("*").alias("all"),
        F.sum(F.when(F.col("f").isNull(), 1).otherwise(0)).alias("never"),
        F.sum(F.when(F.col("f") == 1, 1).otherwise(0)).alias("inside"),
    ).first()
    st.done("(block, mmsi) groups", g["all"])
    st.counts.update(groups_never_enter=int(g["never"]), groups_start_inside=int(g["inside"]))
    print(f"[r1]   never enter {g['never']}, start inside the zone {g['inside']}")
    blocks = flagged.select("block").distinct().count()
    st.counts["blocks"] = blocks

    arrivals, stops, _ = r1.detect(a)
    arrivals = arrivals.persist()
    stops = stops.persist()
    st.done("arrivals", arrivals.count())
    st.counts["arrivals_large"] = arrivals.where("large").count()
    st.counts["moored_stops_elsewhere"] = int(
        arrivals.agg(F.sum("moored_stops_elsewhere")).first()[0] or 0
    )
    st.done("stops (>= 30 min, not moored)", stops.count())
    st.counts["anchored_stops"] = stops.where("anchored").count()

    msgs = r1.eta_messages(std.unionByName(quar.drop("reject_reason")), days)
    eta = r1.eta_at_entry(arrivals, msgs).persist()
    st.done("ETA rows (large arrivals)", eta.count())
    e = eta.agg(
        F.count("eta").alias("with_eta"),
        F.sum(F.col("kept_7d").cast("int")).alias("kept"),
        F.sum(F.col("eta_tie_conflict").cast("int")).alias("tie_conflict"),
    ).first()
    st.counts.update(
        eta_with=int(e["with_eta"]),
        eta_kept_7d=int(e["kept"] or 0),
        eta_tie_conflict=int(e["tie_conflict"] or 0),
    )
    print(
        f"[r1]   with ETA {e['with_eta']}, kept <= 7 d {e['kept']}, "
        f"ETA ties with different values {e['tie_conflict']}"
    )

    arrivals.write.mode("overwrite").parquet(r1.curated_path("esbjerg_arrivals"))
    stops.write.mode("overwrite").parquet(r1.curated_path("esbjerg_stops"))
    eta.write.mode("overwrite").parquet(r1.curated_path("esbjerg_eta_at_entry"))
    st.done("write curated tables")

    total = round(time.time() - st.t0, 1)
    m = {
        "run_id": args.run_id,
        "days": days,
        "blocks_arg": args.blocks,
        "counts": st.counts,
        "seconds": st.seconds,
        "runtime_s": total,
    }
    path = r1.curated_path(f"_manifests/esbjerg_r1/{args.run_id}")
    spark.createDataFrame([(json.dumps(m),)], "value string").coalesce(1).write.mode(
        "overwrite"
    ).text(path)
    print(f"[r1] counts {json.dumps(st.counts)}")
    print(f"[r1] manifest {path}")
    print(f"[r1] runtime {total} s")
    spark.stop()


if __name__ == "__main__":
    main()
