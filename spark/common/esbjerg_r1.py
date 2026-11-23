"""R1: the October Esbjerg waiting analysis, rebuilt with Spark DataFrames and window functions.

A second, independent implementation of `port_waiting` and `load_port_class_a` in
check_datasets.py, the large-ship rule in year_analysis.py and the ETA table in make_figures.py
(the October code, kept outside the repo). Every rule cites its line in that code, so the two can be
compared rule by rule:

    cd = check_datasets.py, ya = year_analysis.py, mf = make_figures.py

Nothing here changes the October method. Rules to revisit are listed for R2 in
docs/spark-milestones.md.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import lake_path

# ---------------------------------------------------------------- constants (October values)
PORT_ZONE = (55.440, 8.405, 55.490, 8.490)  # cd:53  lat_min, lon_min, lat_max, lon_max
NORDBY_ZONE = (55.440, 8.395, 55.462, 8.418)  # cd:55  Fanoe ferry harbour, the "ignore" zone
STATIONARY_SOG = 0.5  # cd:57  knots
MIN_WAIT_MIN = 30  # cd:58  minimum stop length, and the gap that breaks a run (cd:1081)
ANCHOR_SHARE = 0.1  # cd:1003
LARGE_LENGTH = 90  # cd:1004
NOT_CARGO_TYPES = (  # cd:1005
    "Passenger",
    "HSC",
    "Military",
    "Law enforcement",
    "Pilot",
    "SAR",
    "Tug",
    "Port tender",
    "Fishing",
    "Sailing",
    "Pleasure",
)
LARGE_TYPES = ("Cargo", "Tanker")  # ya:37
MOORED_STOP_SHARE = 0.5  # cd:1006
THIN_CHUNK_ROWS = 500_000  # cd:1019  pandas chunksize: minute thinning runs per chunk
ETA_TOLERANCE_S = 3 * 24 * 3600  # mf:247  merge_asof tolerance 3 days
ETA_MAX_ABS_H = 7 * 24  # mf:258  the 7-day exclusion
ETA_FORMAT = "dd/MM/yyyy HH:mm:ss"  # mf:244
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"  # the portarea caches store str(pd.Timestamp)

# The header of every portarea_<day>.csv.gz (written by cd.scan_dma_zip, cd:343).
CSV_COLUMNS = (
    "ts",
    "mobile",
    "mmsi",
    "lat",
    "lon",
    "nav",
    "sog",
    "cog",
    "heading",
    "imo",
    "name",
    "shiptype",
    "cargotype",
    "width",
    "length",
    "draught",
    "dest",
    "eta",
)
NUMERIC_COLUMNS = ("lat", "lon", "sog", "cog", "heading", "width", "length", "draught")
STRING_COLUMNS = tuple(c for c in CSV_COLUMNS if c not in NUMERIC_COLUMNS and c != "ts")
# pandas read_csv turns these strings into NaN by default (its na_values list). The October code
# reads with the defaults, so a ship named "NA" has no name there. Kept for parity.
PANDAS_NA_TOKENS = (
    "",
    "#N/A",
    "#N/A N/A",
    "#NA",
    "-1.#IND",
    "-1.#QNAN",
    "-NaN",
    "-nan",
    "1.#IND",
    "1.#QNAN",
    "<NA>",
    "N/A",
    "NA",
    "NULL",
    "NaN",
    "None",
    "n/a",
    "nan",
    "null",
)

RAW_CSV_DDL = ", ".join(f"`{c}` STRING" for c in CSV_COLUMNS)
# pandas to_csv doubles a quote inside a quoted field (""), Spark's default escape is a backslash
CSV_OPTIONS = {"quote": '"', "escape": '"'}

STANDARDIZED_SCHEMA = T.StructType(
    [
        T.StructField("ts", T.TimestampType()),
        T.StructField("mobile", T.StringType()),
        T.StructField("mmsi", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("nav", T.StringType()),
        T.StructField("sog", T.DoubleType()),
        T.StructField("cog", T.DoubleType()),
        T.StructField("heading", T.DoubleType()),
        T.StructField("imo", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("shiptype", T.StringType()),
        T.StructField("cargotype", T.StringType()),
        T.StructField("width", T.DoubleType()),
        T.StructField("length", T.DoubleType()),
        T.StructField("draught", T.DoubleType()),
        T.StructField("dest", T.StringType()),
        T.StructField("eta", T.StringType()),
        T.StructField("ts_raw", T.StringType()),
        T.StructField("day", T.StringType()),  # the file's day (portarea_<day>.csv.gz)
        T.StructField("line_no", T.LongType()),  # 0-based data record ordinal in the file
        T.StructField("chunk", T.LongType()),  # line_no // THIN_CHUNK_ROWS
    ]
)


# ---------------------------------------------------------------- lake paths and day lists
def standardized_path() -> str:
    return lake_path("standardized", "ais_positions/source=portarea")


def quarantine_path() -> str:
    return lake_path("quarantine", "ais_positions/source=portarea")


def curated_path(name: str) -> str:
    return lake_path("curated", name)


def blocks_of(days: Sequence[str]) -> dict[str, int]:
    """cd:1050-1058 in plain Python, to pick blocks of the day list on the command line."""
    ds = sorted(date.fromisoformat(d) for d in days)
    out, b = {}, 0
    for i, d in enumerate(ds):
        if i and (d - ds[i - 1]).days > 1:
            b += 1
        out[d.isoformat()] = b
    return out


def select_days(days_file: str, blocks: Sequence[int] | None = None) -> list[str]:
    """The days of year_comparison.json ["days"], optionally only some blocks of them."""
    with open(days_file) as f:
        days = list(json.load(f)["days"])
    if blocks is None:
        return days
    bl = blocks_of(days)
    return [d for d in days if bl[d] in set(blocks)]


QUARANTINE_SCHEMA = T.StructType(
    [*STANDARDIZED_SCHEMA.fields, T.StructField("reject_reason", T.StringType())]
)


def read_lake(
    spark: SparkSession, path: str, schema: T.StructType = STANDARDIZED_SCHEMA
) -> DataFrame:
    """Read a day-partitioned lake table with its explicit schema (`day` stays a string).

    An explicit schema also reads a table with no data files (no rejects) as empty.
    """
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    return spark.read.schema(schema).parquet(path)


# ---------------------------------------------------------------- ingestion
def _na_to_null(c: Column) -> Column:
    return F.when(c.isin(*PANDAS_NA_TOKENS), F.lit(None).cast("string")).otherwise(c)


def parse_lines(lines: DataFrame) -> DataFrame:
    """Raw lines (day, idx, value) -> typed rows plus `reject_reason` (null when the row is kept).

    `idx` is the zipWithIndex position of the line in its file, header included (idx 0), so
    line_no = idx - 1. Blank lines are dropped here (pandas skips them too); callers count them.
    """
    data = lines.where((F.col("idx") > 0) & (F.trim("value") != ""))
    parsed = data.select(
        "day",
        (F.col("idx") - 1).alias("line_no"),
        F.from_csv("value", RAW_CSV_DDL, CSV_OPTIONS).alias("r"),
    )
    cols: list[Column] = []
    for c in CSV_COLUMNS:
        raw = F.col(f"r.{c}")
        if c == "ts":
            cols.append(F.try_to_timestamp(raw, F.lit(TS_FORMAT)).alias("ts"))
        elif c in NUMERIC_COLUMNS:
            cols.append(_na_to_null(raw).try_cast("double").alias(c))
        else:
            cols.append(_na_to_null(raw).alias(c))
    typed = parsed.select(
        *cols,
        F.col("r.ts").alias("ts_raw"),
        F.col("r.lat").alias("lat_raw"),
        F.col("r.lon").alias("lon_raw"),
        "day",
        "line_no",
        (F.col("line_no") / F.lit(THIN_CHUNK_ROWS)).cast("long").alias("chunk"),
    )
    # cd:1024-1025: pd.to_datetime(errors="coerce"), then dropna(subset=["ts","lat","lon"]).
    reasons = F.array_compact(
        F.array(
            F.when(_na_to_null(F.col("ts_raw")).isNull(), F.lit("ts_missing")),
            F.when(
                _na_to_null(F.col("ts_raw")).isNotNull() & F.col("ts").isNull(),
                F.lit("ts_unparsable"),
            ),
            F.when(_na_to_null(F.col("lat_raw")).isNull(), F.lit("lat_missing")),
            F.when(
                _na_to_null(F.col("lat_raw")).isNotNull() & F.col("lat").isNull(),
                F.lit("lat_unparsable"),
            ),
            F.when(_na_to_null(F.col("lon_raw")).isNull(), F.lit("lon_missing")),
            F.when(
                _na_to_null(F.col("lon_raw")).isNotNull() & F.col("lon").isNull(),
                F.lit("lon_unparsable"),
            ),
        )
    )
    return typed.withColumn(
        "reject_reason",
        F.when(F.size(reasons) > 0, F.concat_ws(";", reasons)),
    ).drop("lat_raw", "lon_raw")


def split_rejects(typed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(standardized rows, quarantined rows with reject_reason)."""
    good = typed.where(F.col("reject_reason").isNull()).select(*STANDARDIZED_SCHEMA.fieldNames())
    bad = typed.where(F.col("reject_reason").isNotNull())
    return good, bad


# ---------------------------------------------------------------- loading (cd:1011-1031)
def class_a_thinned(std: DataFrame, days: Iterable[str]) -> DataFrame:
    """The rows `load_port_class_a` returns: Class A, one row per ship-minute, unique (mmsi, ts).

    cd:1021 mobile == "Class A" (exact). cd:1025 rows without ts/lat/lon are already in quarantine.
    cd:1026 first row per (mmsi, minute) inside each 500k-line chunk of each file.
    cd:1030-1031 stable sort by (mmsi, ts), then the first row per (mmsi, ts) in file order.
    groupby(["block","mmsi"]) drops a null mmsi (pandas dropna=True), so it is dropped here.
    """
    a = std.where(
        F.col("day").isin(list(days)) & (F.col("mobile") == "Class A") & F.col("mmsi").isNotNull()
    )
    minute = Window.partitionBy(
        "day", "chunk", "mmsi", F.date_trunc("minute", "ts").alias("minute")
    ).orderBy("line_no")
    a = a.withColumn("_k", F.row_number().over(minute)).where("_k = 1").drop("_k")
    same_ts = Window.partitionBy("mmsi", "ts").orderBy("day", "line_no")
    return a.withColumn("_k", F.row_number().over(same_ts)).where("_k = 1").drop("_k")


def tie_counts(std: DataFrame, days: Iterable[str]) -> dict[str, int]:
    """Class A (mmsi, ts) duplicates before thinning, and those that disagree on nav or sog."""
    a = std.where(
        F.col("day").isin(list(days)) & (F.col("mobile") == "Class A") & F.col("mmsi").isNotNull()
    )
    g = a.groupBy("mmsi", "ts").agg(
        F.count("*").alias("n"),
        F.count_distinct(F.coalesce("nav", F.lit("\u0000"))).alias("navs"),
        F.count_distinct(F.coalesce(F.col("sog").cast("string"), F.lit("\u0000"))).alias("sogs"),
    )
    r = g.where("n > 1").agg(
        F.sum(F.col("n") - 1).alias("dup_rows"),
        F.count("*").alias("groups"),
        F.sum(F.when((F.col("navs") > 1) | (F.col("sogs") > 1), 1).otherwise(0)).alias("conflict"),
    )
    row = r.first()
    return {
        "tie_rows": int(row["dup_rows"] or 0),
        "tie_groups": int(row["groups"] or 0),
        "tie_groups_conflicting": int(row["conflict"] or 0),
    }


# ---------------------------------------------------------------- detection (cd:1061-1116)
def in_box(lat: Column, lon: Column, box: tuple[float, float, float, float]) -> Column:
    """cd:108-109 Series.between: both ends inclusive. A null coordinate is outside."""
    return F.coalesce(lat.between(box[0], box[2]) & lon.between(box[1], box[3]), F.lit(False))


def add_blocks(a: DataFrame, day_source: DataFrame | None = None) -> DataFrame:
    """cd:1067-1069 and cd:1050-1058: day = the ts date; consecutive days share a block id.

    The days come from the rows themselves (`day_source` when given: the full data, so that a
    subset of ships gets the same block ids as the whole run).
    """
    a = a.withColumn("ts_day", F.to_date("ts"))
    days = (
        (day_source if day_source is not None else a)
        .select(F.to_date("ts").alias("ts_day"))
        .distinct()
    )
    w = Window.orderBy("ts_day")
    days = days.withColumn(
        "_new",
        F.when(F.datediff("ts_day", F.lag("ts_day").over(w)) > 1, 1).otherwise(0),
    ).withColumn("block", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    return a.join(F.broadcast(days.select("ts_day", F.col("block").cast("int"))), "ts_day")


def ship_types(a: DataFrame) -> DataFrame:
    """cd:1075: per mmsi, the most common type over all loaded rows, not null and not "Undefined".

    Series.mode() returns its values sorted, and .iat[0] takes the first: ties go to the
    alphabetically smallest type.
    """
    counts = (
        a.where(F.col("shiptype").isNotNull() & (F.col("shiptype") != "Undefined"))
        .groupBy("mmsi", "shiptype")
        .count()
    )
    w = Window.partitionBy("mmsi").orderBy(F.col("count").desc(), F.col("shiptype").asc())
    return (
        counts.withColumn("_k", F.row_number().over(w))
        .where("_k = 1")
        .select("mmsi", F.col("shiptype").alias("type"))
    )


def flag_rows(a: DataFrame) -> DataFrame:
    """Per-row flags. a must already carry `block`."""
    in_zone = in_box(F.col("lat"), F.col("lon"), PORT_ZONE)  # cd:1070
    ign = in_box(F.col("lat"), F.col("lon"), NORDBY_ZONE) & ~in_zone  # cd:1071-1074
    anchored = F.coalesce(F.lower("nav").contains("anchor"), F.lit(False))  # cd:1077
    still = F.coalesce(F.col("sog") < STATIONARY_SOG, F.lit(False)) | anchored  # cd:1078
    moored = F.coalesce(F.col("nav") == "Moored", F.lit(False))  # cd:1100, exact
    return (
        a.withColumn("in_zone", in_zone)
        .withColumn("ign", ign)
        .withColumn("anchored", anchored)
        .withColumn("moored", moored)
        .withColumn("wait_flag", still & ~F.col("in_zone") & ~F.col("ign"))  # cd:1079
    )


def annotate_tracks(a: DataFrame) -> DataFrame:
    """Row number, run id and first entry per (block, mmsi), ordered by ts (cd:1082-1095).

    brk (cd:1094): the flag differs from the previous row (always true on the first row, where
    shift() gives NaN), or the gap to the previous row is more than 30 min (exactly 30 does not
    break). It is computed over every row of the group; run_id is its running sum.
    """
    grp = Window.partitionBy("block", "mmsi")
    w = grp.orderBy("ts")
    prev_flag = F.lag("wait_flag").over(w)
    gap_s = F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")
    brk = (
        prev_flag.isNull() | (F.col("wait_flag") != prev_flag) | (gap_s > MIN_WAIT_MIN * 60)
    ).cast("int")
    return (
        a.withColumn("rn", F.row_number().over(w))
        .withColumn("brk", F.coalesce(brk, F.lit(1)))
        .withColumn("run_id", F.sum("brk").over(w.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("first_rn", F.min(F.when(F.col("in_zone"), F.col("rn"))).over(grp))
    )


def _minutes(start: Column, end: Column) -> Column:
    # pandas: (end - start).total_seconds() / 60
    return (F.unix_micros(end) - F.unix_micros(start)).cast("double") / F.lit(1e6) / F.lit(60.0)


def detect(
    a: DataFrame, day_source: DataFrame | None = None
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Arrivals, stops and the annotated rows of the entering groups, from class_a_thinned rows.

    Returns (arrivals, stops, tracks). `stops` has one row per counted stop (cd:1103-1107);
    arrivals carries the October columns (cd:1114) plus the entry position.
    """
    a = flag_rows(add_blocks(a, day_source))
    types = ship_types(a)
    t = annotate_tracks(a)
    # cd:1084-1086: never entered, or the first row of the group is already in the zone
    t = t.where(F.col("first_rn").isNotNull() & (F.col("first_rn") > 1))

    meta = t.groupBy("block", "mmsi").agg(
        # cd:1088: first non-null name in ts order (min_by skips rows whose ordering is null)
        F.min_by("name", F.when(F.col("name").isNotNull(), F.col("ts"))).alias("name"),
        F.max("length").alias("length"),  # cd:1089
        F.min("ts").alias("track_start"),  # cd:1092
        F.min_by("ts", F.when(F.col("rn") == F.col("first_rn"), F.col("rn"))).alias("arrival"),
        F.min_by("lat", F.when(F.col("rn") == F.col("first_rn"), F.col("rn"))).alias("entry_lat"),
        F.min_by("lon", F.when(F.col("rn") == F.col("first_rn"), F.col("rn"))).alias("entry_lon"),
    )
    meta = meta.join(types, "mmsi", "left").withColumn(
        "type", F.coalesce("type", F.lit("Undefined"))
    )

    # cd:1095: wait rows before the first entry, grouped by run
    runs = (
        t.where((F.col("rn") < F.col("first_rn")) & F.col("wait_flag"))
        .groupBy("block", "mmsi", "run_id")
        .agg(
            F.min("ts").alias("start"),
            F.max("ts").alias("end"),
            F.count("*").alias("messages"),
            # cd:1099, cd:1100: shares of messages; a null nav counts as false (amendment 3)
            F.avg(F.when(F.col("anchored"), 1.0).otherwise(0.0)).alias("anchored_share"),
            F.avg(F.when(F.col("moored"), 1.0).otherwise(0.0)).alias("moored_share"),
            F.percentile("lat", F.lit(0.5)).alias("lat"),  # cd:1104 median (exact)
            F.percentile("lon", F.lit(0.5)).alias("lon"),
            (F.min("rn") == 1).alias("cut_at_start"),  # cd:1106
        )
        .withColumn("minutes", _minutes(F.col("start"), F.col("end")))
        .where(F.col("minutes") >= MIN_WAIT_MIN)  # cd:1097
        .withColumn("moored_elsewhere", F.col("moored_share") >= MOORED_STOP_SHARE)  # cd:1100
    )

    stops = (
        runs.where(~F.col("moored_elsewhere"))
        .join(meta.select("block", "mmsi", "name", "type", "length", "arrival"), ["block", "mmsi"])
        .withColumn("anchored", F.col("anchored_share") >= ANCHOR_SHARE)  # cd:1110, ya:45
        .withColumn("month", F.date_format("arrival", "yyyy-MM"))
        .select(
            "block",
            "mmsi",
            "name",
            "type",
            "length",
            "start",
            "end",
            "minutes",
            "anchored_share",
            "lat",
            "lon",
            "arrival",
            "cut_at_start",
            "anchored",
            "month",
            "moored_share",
            "messages",
        )
    )

    per_arrival = runs.groupBy("block", "mmsi").agg(
        F.sum(F.when(~F.col("moored_elsewhere"), 1).otherwise(0)).alias("stops"),
        F.sum(
            F.when(
                ~F.col("moored_elsewhere") & (F.col("anchored_share") >= ANCHOR_SHARE), 1
            ).otherwise(0)
        ).alias("anchor_stops"),
        F.sum(F.when(~F.col("moored_elsewhere"), F.col("minutes")).otherwise(0.0)).alias(
            "wait_min"
        ),
        F.sum(
            F.when(
                ~F.col("moored_elsewhere") & (F.col("anchored_share") >= ANCHOR_SHARE),
                F.col("minutes"),
            ).otherwise(0.0)
        ).alias("anchor_wait_min"),
        F.sum(F.when(F.col("moored_elsewhere"), 1).otherwise(0)).alias("moored_stops_elsewhere"),
    )

    arrivals = (
        meta.join(per_arrival, ["block", "mmsi"], "left")
        .fillna(0, subset=["stops", "anchor_stops", "moored_stops_elsewhere"])
        .fillna(0.0, subset=["wait_min", "anchor_wait_min"])
        .withColumn("large", large_ship(F.col("type"), F.col("length")))
        .withColumn("month", F.date_format("arrival", "yyyy-MM"))  # ya:38
        .withColumn("waited", F.col("anchor_stops") > 0)  # ya:41
        .withColumn("entry_in_nordby", in_box(F.col("entry_lat"), F.col("entry_lon"), NORDBY_ZONE))
        .select(
            "block",
            "mmsi",
            "name",
            "type",
            "length",
            "arrival",
            "track_start",
            "stops",
            "anchor_stops",
            "wait_min",
            "anchor_wait_min",
            "moored_stops_elsewhere",
            "large",
            "month",
            "waited",
            "entry_lat",
            "entry_lon",
            "entry_in_nordby",
        )
    )
    return arrivals, stops, t


def large_ship(type_col: Column, length: Column) -> Column:
    """ya:37: Cargo or Tanker, or (length, null as 0) >= 90 and not a service/passenger type."""
    return type_col.isin(*LARGE_TYPES) | (
        (F.coalesce(length, F.lit(0.0)) >= LARGE_LENGTH) & ~type_col.isin(*NOT_CARGO_TYPES)
    )


# ---------------------------------------------------------------- ETA (mf:226-262)
def eta_messages(rows: DataFrame, days: Iterable[str]) -> DataFrame:
    """mf:235-245: raw Class A rows with an ETA, before any thinning or coordinate check.

    `rows` is standardized and quarantined rows together (amendment 2): a row without lat/lon still
    carries its ETA. Rows without a parsable ts or ETA are dropped, as pandas dropna does.
    """
    return (
        rows.where(
            F.col("day").isin(list(days))
            & (F.col("mobile") == "Class A")
            & F.col("eta").isNotNull()
            & F.col("ts").isNotNull()
        )
        .withColumn("eta_dt", F.try_to_timestamp("eta", F.lit(ETA_FORMAT)))
        .where(F.col("eta_dt").isNotNull())
        .select("mmsi", F.col("ts").alias("eta_ts"), "eta_dt", "day", "line_no")
    )


def eta_at_entry(arrivals: DataFrame, messages: DataFrame) -> DataFrame:
    """ "ETA reported at entry": the last ETA a ship sent in the 3 days up to its harbour entry.

    mf:247 merge_asof(direction="backward", tolerance=3D): arrival - 3 d <= ts <= arrival.
    Every large arrival is kept, with a null ETA when nothing matches (left join, mf:231-233).
    Equal ts: pandas sorts with an unstable quicksort (mf:245), so its pick is undefined; here
    the last row in file order wins.
    mf:256-258: diff_h = arrival - eta in hours; kept_7d = |diff_h| <= 168.
    """
    la = arrivals.where("large")
    m = messages.alias("m")
    cond = (
        (F.col("a.mmsi") == F.col("m.mmsi"))
        & (F.col("m.eta_ts") <= F.col("a.arrival"))
        & (F.col("m.eta_ts") >= F.col("a.arrival") - F.expr(f"INTERVAL {ETA_TOLERANCE_S} SECONDS"))
    )
    j = la.alias("a").join(m, cond, "left")
    w = Window.partitionBy("a.block", "a.mmsi", "a.arrival").orderBy(
        F.col("m.eta_ts").desc_nulls_last(),
        F.col("m.day").desc_nulls_last(),
        F.col("m.line_no").desc_nulls_last(),
    )
    tied = Window.partitionBy("a.block", "a.mmsi", "a.arrival", "m.eta_ts")
    j = (
        j.withColumn("_k", F.row_number().over(w))
        .withColumn("_tie_conflict", F.min("m.eta_dt").over(tied) != F.max("m.eta_dt").over(tied))
        .where("_k = 1")
    )
    diff_h = (F.unix_micros("a.arrival") - F.unix_micros("m.eta_dt")).cast("double") / 3.6e9
    return j.select(
        "a.*",
        F.col("m.eta_ts").alias("ts"),
        F.col("m.eta_dt").alias("eta"),
        diff_h.alias("diff_h"),
        (F.abs(diff_h) <= ETA_MAX_ABS_H).alias("kept_7d"),
        F.coalesce(F.col("_tie_conflict"), F.lit(False)).alias("eta_tie_conflict"),
    )
