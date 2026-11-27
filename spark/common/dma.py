"""DMA daily AIS files -> the standardized rows of one port, in the schema of the October files.

The rules of jobs/clean_dma.py, kept apart from its I/O so they can be tested on a small frame.

Order of the work, so a shuffle only ever sees the port's rows (about 380 thousand of the 21
million rows of a day for Esbjerg):

1. `tag`: per row, without any shuffle: is it a vessel (Class A or Class B), does it have a
   possible position, is it inside the port's bbox (config/ports.yaml).
2. `candidates`: still without a shuffle, keep the vessel rows that are inside the box or have
   no possible position. Everything else is dropped here.
3. `port_rows`: the rows inside the box, plus the rows without a possible position of the ships
   seen inside the box that day (a broadcast semi-join, so the large side is not shuffled).
4. `standardize`: typed columns, reasons, the file order (`line_no`) and exact duplicates. The
   two windows here are the only shuffles, and they see only the rows of step 3.

A row goes to quarantine with every reason that applies, joined by ";":
    mmsi_invalid         not nine digits starting with 2 to 7 (a ship's MMSI)
    position_impossible  latitude or longitude missing, unparsable or outside +-90 / +-180
                         (DMA writes 91 and 181 for "not available")
    ts_unparsable        the timestamp is missing or not dd/MM/yyyy HH:mm:ss
    duplicate            all 26 fields equal, as text, an earlier row of the same day (the
                         earlier one, in file order, is kept). Two rows of one ship in the same
                         second that differ in any field are not duplicates: both are kept,
                         and the arrivals analysis keeps the first (class_a_thinned)
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import esbjerg_r1 as r1
from common.config import BBox

# The 26 columns of a DMA file, in file order, with the short names used here.
DMA_HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,Heading,"
    "IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,Draught,"
    "Destination,ETA,Data source type,A,B,C,D"
)
DMA_COLUMNS = (
    "ts",
    "mobile",
    "mmsi",
    "lat",
    "lon",
    "nav",
    "rot",
    "sog",
    "cog",
    "heading",
    "imo",
    "callsign",
    "name",
    "shiptype",
    "cargotype",
    "width",
    "length",
    "posfix",
    "draught",
    "dest",
    "eta",
    "source_type",
    "a",
    "b",
    "c",
    "d",
)
CORRUPT = "_corrupt"  # the raw line, when it does not split into 26 fields
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in (*DMA_COLUMNS, CORRUPT)])
CSV_OPTIONS = {
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT,
    "quote": '"',
    "escape": '"',
}
TS_FORMAT = "dd/MM/yyyy HH:mm:ss"  # UTC: the session time zone is UTC
VESSELS = ("Class A", "Class B")
MMSI_PATTERN = r"^[2-7][0-9]{8}$"
REASONS = ("mmsi_invalid", "position_impossible", "ts_unparsable", "duplicate")
# File order: the byte offset of the split a row was read from, then its position in the task.
ORDER = ("_block", "_seq")


def read_raw(spark: SparkSession, path: str, day: str) -> DataFrame:
    """One landing file as 26 string columns plus `day` and its file-order keys.

    The CSV is plain text, so Spark splits it into many tasks. Within a file, (`_block`, `_seq`)
    sorts the rows in file order: `_block` is the byte offset of the split, and
    monotonically_increasing_id grows with the position inside each task.

    CSV column pruning is turned off for the session: with it, Spark parses only the columns a
    query uses, and a row with too few fields is then not seen as malformed. The job needs every
    column anyway.
    """
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    df = spark.read.schema(RAW_SCHEMA).options(**CSV_OPTIONS).csv(path)
    return df.select(
        *DMA_COLUMNS,
        CORRUPT,
        F.lit(day).alias("day"),
        F.col("_metadata.file_block_start").alias("_block"),
        F.monotonically_increasing_id().alias("_seq"),
    )


def _double(c: Column) -> Column:
    return r1._na_to_null(c, r1.DMA_NA_TOKENS).try_cast("double")


def tag(raw: DataFrame, box: BBox) -> DataFrame:
    """Per-row flags, no shuffle: `vessel`, `position_ok`, `in_box`, `malformed`."""
    lat, lon = _double(F.col("lat")), _double(F.col("lon"))
    position_ok = F.coalesce(lat.between(-90.0, 90.0) & lon.between(-180.0, 180.0), F.lit(False))
    in_box = position_ok & r1.in_box(lat, lon, (box.min_lat, box.min_lon, box.max_lat, box.max_lon))
    return (
        raw.withColumn("malformed", F.col(CORRUPT).isNotNull())
        .withColumn("vessel", F.coalesce(F.col("mobile").isin(*VESSELS), F.lit(False)))
        .withColumn("position_ok", position_ok)
        .withColumn("in_box", in_box)
    )


def counts(tagged: DataFrame) -> list[Column]:
    """Aggregates for DataFrame.observe: the day's numbers, gathered during the one scan."""
    vessel = F.col("vessel") & ~F.col("malformed")

    def n(cond: Column, name: str) -> Column:
        return F.sum(F.when(cond, 1).otherwise(0)).alias(name)

    return [
        F.count(F.lit(1)).alias("rows_in"),
        n(F.col("malformed"), "rows_malformed"),
        n(vessel, "vessel_rows"),
        n(vessel & F.col("in_box"), "vessel_rows_in_box"),
        n(vessel & ~F.col("position_ok"), "vessel_rows_without_position"),
    ]


def candidates(tagged: DataFrame) -> DataFrame:
    """Vessel rows inside the box or without a possible position; no shuffle."""
    return tagged.where(
        F.col("vessel") & ~F.col("malformed") & (F.col("in_box") | ~F.col("position_ok"))
    ).drop(CORRUPT, "malformed", "vessel")


def port_rows(cand: DataFrame) -> DataFrame:
    """The box rows, plus the no-position rows of ships seen in the box that day."""
    inside = cand.where("in_box")
    ships = inside.select("day", "mmsi").distinct()
    lost = cand.where(~F.col("position_ok")).join(F.broadcast(ships), ["day", "mmsi"], "left_semi")
    return inside.unionByName(lost)


def standardize(rows: DataFrame) -> DataFrame:
    """Typed rows in STANDARDIZED_SCHEMA order, plus `reject_reason` (null when kept).

    line_no is the 0-based position of the row among the day's port rows, in file order,
    quarantined rows included: the same meaning as in the October portarea files, which held
    the rows of one area in DMA file order. chunk = line_no // 500,000, as there.
    """
    first = Window.partitionBy("day", *DMA_COLUMNS).orderBy(*ORDER)
    in_file = Window.partitionBy("day").orderBy(*ORDER)
    rows = rows.withColumn("_copy", F.row_number().over(first)).withColumn(
        "line_no", (F.row_number().over(in_file) - 1).cast("long")
    )

    def text(c: str) -> Column:
        return r1._na_to_null(F.col(c), r1.DMA_NA_TOKENS)

    typed = rows.select(
        F.try_to_timestamp(F.col("ts"), F.lit(TS_FORMAT)).alias("ts"),
        text("mobile").alias("mobile"),
        text("mmsi").alias("mmsi"),
        _double(F.col("lat")).alias("lat"),
        _double(F.col("lon")).alias("lon"),
        text("nav").alias("nav"),
        *(_double(F.col(c)).alias(c) for c in ("sog", "cog", "heading")),
        *(text(c).alias(c) for c in ("imo", "name", "shiptype", "cargotype")),
        *(_double(F.col(c)).alias(c) for c in ("width", "length", "draught")),
        text("dest").alias("dest"),
        text("eta").alias("eta"),
        F.col("ts").alias("ts_raw"),
        "day",
        "line_no",
        (F.col("line_no") / F.lit(r1.THIN_CHUNK_ROWS)).cast("long").alias("chunk"),
        "position_ok",
        "_copy",
    )
    reasons = F.array_compact(
        F.array(
            F.when(~F.coalesce(F.col("mmsi").rlike(MMSI_PATTERN), F.lit(False)), "mmsi_invalid"),
            F.when(~F.col("position_ok"), "position_impossible"),
            F.when(F.col("ts").isNull(), "ts_unparsable"),
            F.when(F.col("_copy") > 1, "duplicate"),
        )
    )
    return typed.withColumn(
        "reject_reason", F.when(F.size(reasons) > 0, F.concat_ws(";", reasons))
    ).select(*r1.STANDARDIZED_SCHEMA.fieldNames(), "reject_reason")
