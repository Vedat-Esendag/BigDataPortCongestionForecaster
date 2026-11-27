"""DMA landing CSV -> standardized Esbjerg rows: the rules of common/dma.py and jobs/clean_dma.py.

fixtures/dma_sample.csv is a DMA day in miniature, one row per case (index = data row):
    0 kept                        9 unparsable time
    1 kept, "Unknown" -> null    10 missing time
    2 exact duplicate of 0       11 unparsable latitude, ship seen in the box
    3 kept, Class B              12 malformed: 5 fields
    4 Base Station: dropped      13 invalid MMSI and exact duplicate of 6
    5 outside the box: dropped   14 SAR Airborne: dropped
    6 invalid MMSI (7 digits)    15 on the box corner: inside, kept
    7 lat 91 / lon 181, ship seen in the box
    8 lat 91 / lon 181, ship never in the box: dropped
   16 kept, quoted name with a comma
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import Observation
from pyspark.sql import functions as F

from common import dma
from common import esbjerg_r1 as r1
from common.config import get_port, load_ports

FIXTURE = Path(__file__).parent / "fixtures" / "dma_sample.csv"
DAY = "2026-10-01"
BOX = get_port(load_ports(), "dkebj").bbox
# data row of the fixture -> line_no among the port rows (file order)
PORT_ROWS = {0: 0, 1: 1, 2: 2, 3: 3, 6: 4, 7: 5, 9: 6, 10: 7, 11: 8, 13: 9, 15: 10, 16: 11}
KEPT = {0, 1, 3, 15, 16}
QUARANTINED = {
    2: "duplicate",
    6: "mmsi_invalid",
    7: "position_impossible",
    9: "ts_unparsable",
    10: "ts_unparsable",
    11: "position_impossible",
    13: "mmsi_invalid;duplicate",
}


@pytest.fixture
def small_splits(spark):
    """Read the fixture in many small splits, so file order has to survive them."""
    keys = ("spark.sql.files.maxPartitionBytes", "spark.sql.files.openCostInBytes")
    old = {k: spark.conf.get(k) for k in keys}
    spark.conf.set("spark.sql.files.maxPartitionBytes", "700")
    spark.conf.set("spark.sql.files.openCostInBytes", "0")
    yield
    for k, v in old.items():
        spark.conf.set(k, v)


def standardized(spark):
    raw = dma.read_raw(spark, str(FIXTURE), DAY)
    return dma.standardize(dma.port_rows(dma.candidates(dma.tag(raw, BOX))))


def test_the_header_and_columns_match_a_dma_file():
    assert FIXTURE.read_text().splitlines()[0] == dma.DMA_HEADER
    assert len(dma.DMA_HEADER.split(",")) == len(dma.DMA_COLUMNS) == 26


def test_the_box_covers_the_harbour_and_the_anchorage():
    lat_min, lon_min, lat_max, lon_max = r1.PORT_ZONE
    assert BOX.min_lat <= lat_min and lat_max <= BOX.max_lat
    assert BOX.min_lon <= lon_min and lon_max <= BOX.max_lon
    anchorage = get_port(load_ports(), "dkebj").anchorage
    assert all(
        BOX.min_lat <= lat <= BOX.max_lat and BOX.min_lon <= lon <= BOX.max_lon
        for lon, lat in anchorage
    )


def test_counts_are_gathered_in_the_same_scan(spark):
    tagged = dma.tag(dma.read_raw(spark, str(FIXTURE), DAY), BOX)
    obs = Observation("t")
    cand = dma.candidates(tagged.observe(obs, *dma.counts(tagged)))
    assert cand.count() == 13  # 10 in the box + 3 vessel rows without a position
    assert obs.get == {
        "rows_in": 17,
        "rows_malformed": 1,
        "vessel_rows": 14,
        "vessel_rows_in_box": 10,
        "vessel_rows_without_position": 3,
    }


def test_rows_are_kept_or_quarantined_with_their_reasons(spark, small_splits):
    typed = standardized(spark)
    rows = {r["line_no"]: r for r in typed.collect()}
    by_line = {line: row for row, line in PORT_ROWS.items()}
    assert set(rows) == set(by_line)  # every port row once, line_no 0..11 without gaps
    for line, r in rows.items():
        expected = QUARANTINED.get(by_line[line])
        assert r["reject_reason"] == expected, (by_line[line], r["reject_reason"])
    kept = {by_line[line] for line, r in rows.items() if r["reject_reason"] is None}
    assert kept == KEPT


def test_file_order_survives_many_splits(spark, small_splits):
    raw = dma.read_raw(spark, str(FIXTURE), DAY)
    assert raw.rdd.getNumPartitions() > 3
    typed = standardized(spark).orderBy("line_no")
    minutes = [r["ts_raw"] for r in typed.collect()]
    assert minutes[:4] == [
        "01/10/2026 06:00:00",
        "01/10/2026 06:01:00",
        "01/10/2026 06:00:00",
        "01/10/2026 06:02:00",
    ]
    assert minutes[-1] == "01/10/2026 06:11:00"


def test_values_are_typed_like_the_october_files(spark):
    typed = standardized(spark)
    good, bad = r1.split_rejects(typed)

    def fields(schema):  # Parquet stores every column as nullable, so compare names and types
        return [(f.name, f.dataType) for f in schema.fields]

    assert fields(good.schema) == fields(r1.STANDARDIZED_SCHEMA)
    assert fields(bad.drop("reject_reason").schema) == fields(r1.STANDARDIZED_SCHEMA)
    rows = {r["line_no"]: r for r in good.collect()}
    first, na, b, corner, quoted = (rows[PORT_ROWS[i]] for i in (0, 1, 3, 15, 16))
    assert first["ts"] == datetime(2026, 10, 1, 6, 0, 0)  # dd/MM/yyyy, UTC
    assert (first["mmsi"], first["lat"], first["lon"], first["sog"]) == (
        "219000001",
        55.42,
        8.12,
        10.2,
    )
    assert (first["heading"], first["length"], first["draught"]) == (94.0, 120.0, 6.5)
    assert first["eta"] == "02/10/2026 06:00:00" and first["chunk"] == 0
    assert first["cargotype"] is None  # empty -> null
    assert (na["name"], na["shiptype"], na["imo"], na["dest"], na["eta"]) == (None,) * 5
    assert b["mobile"] == "Class B" and b["nav"] == "Unknown value"  # a real AIS status
    assert (corner["lat"], corner["lon"]) == (55.70, 8.60)
    assert quoted["name"] == "ALPHA, THE"
    assert {r["day"] for r in rows.values()} == {DAY}


def test_impossible_positions_keep_their_values_in_quarantine(spark):
    bad = standardized(spark).where(F.col("reject_reason") == "position_impossible")
    got = {(r["lat"], r["lon"]) for r in bad.collect()}
    assert got == {(None, 8.12), (91.0, 181.0)}


def test_clean_day_writes_the_lake_and_replaces_a_day(spark, tmp_path, monkeypatch):
    from jobs import clean_dma

    monkeypatch.setenv("HDFS_URI", "file://")
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    landing = f"file://{tmp_path}/landing"
    target = tmp_path / "landing" / f"day={DAY}" / f"aisdk-{DAY}.csv"
    target.parent.mkdir(parents=True)
    shutil.copy(FIXTURE, target)

    for _ in range(2):  # the second run replaces the first, it never adds to it
        log = clean_dma.clean_day(spark, landing, DAY, BOX, "test")

    std = r1.read_lake(spark, r1.standardized_path("dma"))
    # what is on disk: the same columns and types; Spark lists the partition column `day` last,
    # and the analysis reads with the explicit schema (r1.read_lake), as for portarea
    on_disk = spark.read.parquet(r1.standardized_path("dma")).schema
    assert sorted((f.name, f.dataType) for f in on_disk.fields) == sorted(
        (f.name, f.dataType) for f in r1.STANDARDIZED_SCHEMA.fields
    )
    quar = r1.read_lake(spark, r1.quarantine_path("dma"), r1.QUARANTINE_SCHEMA)
    assert std.count() == len(KEPT) and quar.count() == len(QUARANTINED)
    assert log["rows_in"] == 17 and log["port_rows"] == 12 and log["balanced"]
    assert (log["rows_kept"], log["rows_quarantined"], log["distinct_mmsi"]) == (5, 7, 2)
    assert log["quarantined_by_reason"] == {
        "mmsi_invalid": 2,
        "position_impossible": 2,
        "ts_unparsable": 2,
        "duplicate": 2,
    }
    assert log["landing_bytes"] == FIXTURE.stat().st_size and log["standardized_bytes"] > 0
    stored = spark.read.json(r1.lake_path("standardized", "_loads/source=dma")).collect()
    assert len(stored) == 1 and stored[0]["rows_kept"] == 5 and stored[0]["day"] == DAY
    assert json.loads(json.dumps(log))["source"] == "dma"
    # the portarea tables are not touched
    assert not (tmp_path / "lake" / "standardized" / "ais_positions" / "source=portarea").exists()


def test_a_file_that_is_not_dma_is_refused(spark, tmp_path):
    from jobs import clean_dma

    other = tmp_path / "other.csv"
    other.write_text("ts,mobile\n1,2\n")
    with pytest.raises(ValueError, match="not a DMA file"):
        clean_dma.check_header(spark, f"file://{other}")
