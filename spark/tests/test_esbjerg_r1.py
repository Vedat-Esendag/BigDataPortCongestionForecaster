"""R1: hand-made tracks through the Spark port of the October Esbjerg waiting method."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from common import esbjerg_r1 as r1

SEA = (55.40, 8.20)  # outside the zone, moving
ANCHORAGE = (55.42, 8.12)  # the Esbjerg anchorage
PORT = (55.465, 8.44)  # inside PORT_ZONE, outside NORDBY_ZONE
NORDBY_STRIP = (55.45, 8.400)  # inside NORDBY_ZONE, outside PORT_ZONE: the only ignored area
NORDBY_DOCK = (55.45, 8.41)  # inside both zones
T0 = datetime(2025, 11, 8, 6, 0, 0)
DAYS = [f"2025-11-{d:02d}" for d in range(8, 15)]


class Track:
    """Builds standardized rows (one ship or several) with file order as written."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, mmsi, ts, pos, sog=0.0, nav=None, **extra):
        line_no = extra.pop("line_no", len(self.rows))
        row = {c: None for c in r1.STANDARDIZED_SCHEMA.fieldNames()}
        row.update(
            ts=ts,
            mobile="Class A",
            mmsi=mmsi,
            lat=pos[0],
            lon=pos[1],
            nav=nav,
            sog=sog,
            name="SHIP " + mmsi,
            shiptype="Cargo",
            length=120.0,
            ts_raw=ts.strftime("%Y-%m-%d %H:%M:%S"),
            day=ts.strftime("%Y-%m-%d"),
            line_no=line_no,
            chunk=line_no // r1.THIN_CHUNK_ROWS,
        )
        row.update(extra)
        self.rows.append(row)
        return self

    def still(self, mmsi, start, minutes, step=10, pos=ANCHORAGE, nav="At anchor"):
        """Stationary rows from start to start + minutes, every `step` minutes, ends included."""
        for m in range(0, minutes + 1, step):
            self.add(mmsi, start + timedelta(minutes=m), pos, 0.1, nav)
        return self

    def df(self, spark: SparkSession):
        return spark.createDataFrame([Row(**r) for r in self.rows], r1.STANDARDIZED_SCHEMA)


def run(spark, track: Track, days=DAYS):
    arrivals, stops, _ = r1.detect(r1.class_a_thinned(track.df(spark), days))
    arr = {r["mmsi"]: r.asDict() for r in arrivals.collect()}
    st = [r.asDict() for r in stops.orderBy("mmsi", "start").collect()]
    return arr, st


def m(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


# ---------------------------------------------------------------- the cases in the brief
def test_anchors_then_enters(spark):
    t = Track().add("1", m(0), SEA, 10.0, "Under way using engine")
    t.still("1", m(10), 120)
    t.add("1", m(140), SEA, 8.0).add("1", m(160), PORT, 3.0)
    arr, st = run(spark, t)
    a = arr["1"]
    assert a["arrival"] == m(160) and a["track_start"] == m(0)
    assert (a["stops"], a["anchor_stops"]) == (1, 1)
    assert a["wait_min"] == pytest.approx(120) and a["anchor_wait_min"] == pytest.approx(120)
    assert a["waited"] and a["large"]
    assert st[0]["anchored_share"] == 1.0 and not st[0]["cut_at_start"]
    assert (st[0]["lat"], st[0]["lon"]) == ANCHORAGE


def test_enters_directly(spark):
    t = Track().add("2", m(0), SEA, 10.0).add("2", m(10), PORT, 5.0)
    arr, st = run(spark, t)
    assert arr["2"]["stops"] == 0 and arr["2"]["arrival"] == m(10) and st == []


def test_nordby_ferry(spark):
    """Waiting in the Nordby strip is ignored; docking at Nordby counts as entering the port
    zone, because NORDBY_ZONE overlaps PORT_ZONE (kept as in October, flagged for R2)."""
    t = Track().add("3", m(0), SEA, 10.0)
    t.still("3", m(10), 60, pos=NORDBY_STRIP, nav="Moored")
    t.add("3", m(80), NORDBY_DOCK, 0.0, "Moored")
    arr, st = run(spark, t)
    assert arr["3"]["stops"] == 0 and arr["3"]["moored_stops_elsewhere"] == 0
    assert arr["3"]["arrival"] == m(80) and arr["3"]["entry_in_nordby"]
    assert st == []


def test_stop_shorter_than_30_min_is_not_counted(spark):
    t = Track()
    for mmsi, length in (("29", 29), ("30", 30)):
        t.add(mmsi, m(0), SEA, 10.0)
        t.still(mmsi, m(1), length, step=1)
        t.add(mmsi, m(length + 2), SEA, 8.0).add(mmsi, m(length + 10), PORT, 3.0)
    arr, _ = run(spark, t)
    assert arr["29"]["stops"] == 0
    assert arr["30"]["stops"] == 1 and arr["30"]["wait_min"] == pytest.approx(30)


def test_gap_of_exactly_30_min_does_not_break_but_31_does(spark):
    t = Track()
    for mmsi, gap in (("g30", 30), ("g31", 31)):
        t.add(mmsi, m(0), SEA, 10.0)
        s = 10
        for off in (0, 10, 10 + gap, 10 + gap + 30):
            t.add(mmsi, m(s + off), ANCHORAGE, 0.0, "At anchor")
        t.add(mmsi, m(s + 10 + gap + 40), SEA, 8.0).add(mmsi, m(s + 10 + gap + 60), PORT, 3.0)
    arr, st = run(spark, t)
    # 30: one run of 70 min. 31: a 10-min run (too short) and a 30-min run.
    assert arr["g30"]["stops"] == 1 and arr["g30"]["wait_min"] == pytest.approx(70)
    assert arr["g31"]["stops"] == 1 and arr["g31"]["wait_min"] == pytest.approx(30)
    assert [s["start"] for s in st if s["mmsi"] == "g31"] == [m(10 + 41)]


def test_stop_spanning_midnight(spark):
    start = datetime(2025, 11, 8, 23, 40)
    t = Track().add("4", start - timedelta(minutes=10), SEA, 10.0)
    t.still("4", start, 60)
    t.add("4", start + timedelta(minutes=70), PORT, 3.0)
    arr, st = run(spark, t)
    assert arr["4"]["block"] == 0 and arr["4"]["stops"] == 1
    assert st[0]["minutes"] == pytest.approx(60) and st[0]["end"].day == 9


def test_ship_starting_inside_port_is_skipped(spark):
    t = Track().add("5", m(0), PORT, 0.0, "Moored").add("5", m(30), SEA, 10.0)
    t.still("5", m(40), 60).add("5", m(120), PORT, 3.0)
    arr, _ = run(spark, t)
    assert "5" not in arr


def test_repeat_visit_counts_only_the_first_entry(spark):
    t = Track().add("6", m(0), SEA, 10.0).add("6", m(10), PORT, 3.0)
    t.add("6", m(50), SEA, 10.0)
    t.still("6", m(60), 120)
    t.add("6", m(200), PORT, 3.0)
    arr, st = run(spark, t)
    assert arr["6"]["arrival"] == m(10) and arr["6"]["stops"] == 0 and st == []


def test_duplicate_timestamps_keep_the_first_row_in_file_order(spark):
    t = Track().add("7", m(0), ANCHORAGE, 0.0, "At anchor", line_no=5)
    t.add("7", m(0), SEA, 10.0, "Under way using engine", line_no=6)
    t.add("7", m(0) + timedelta(seconds=30), SEA, 9.0, line_no=7)  # same minute: thinned
    t.add("7", m(1), SEA, 9.0, line_no=8)
    df = t.df(spark)
    kept = r1.class_a_thinned(df, DAYS).orderBy("ts").collect()
    assert [r["line_no"] for r in kept] == [5, 8]
    ties = r1.tie_counts(df, DAYS)
    assert ties == {"tie_rows": 1, "tie_groups": 1, "tie_groups_conflicting": 1}


# ---------------------------------------------------------------- further rules
def test_minute_thinning_runs_per_500k_chunk(spark):
    """cd:1026 thins inside each pandas chunk, so the same minute on both sides of record
    499,999 / 500,000 keeps two rows."""
    t = Track().add("8", m(0), SEA, 10.0, line_no=499_999)
    t.add("8", m(0) + timedelta(seconds=20), SEA, 10.0, line_no=500_000)
    t.add("8", m(0) + timedelta(seconds=40), SEA, 10.0, line_no=500_001)
    kept = r1.class_a_thinned(t.df(spark), DAYS).orderBy("line_no").collect()
    assert [r["line_no"] for r in kept] == [499_999, 500_000]


def test_mostly_moored_stop_is_a_berth_call_elsewhere(spark):
    t = Track().add("9", m(0), SEA, 10.0)
    navs = ["Moored", "Moored", "Moored", None, None, None, "Moored"]  # 4 of 7 rows
    for i, nav in enumerate(navs):
        t.add("9", m(10 + 10 * i), ANCHORAGE, 0.0, nav)
    t.add("9", m(90), SEA, 8.0).add("9", m(100), PORT, 3.0)
    arr, st = run(spark, t)
    assert arr["9"]["moored_stops_elsewhere"] == 1 and arr["9"]["stops"] == 0 and st == []


def test_anchored_share_counts_null_nav_in_the_denominator(spark):
    t = Track()
    for mmsi, rows in (("a10", 10), ("a11", 11)):  # 1/10 = 0.1 anchored, 1/11 not
        t.add(mmsi, m(0), SEA, 10.0)
        for i in range(rows):
            t.add(mmsi, m(10 + 5 * i), ANCHORAGE, 0.0, "At anchor" if i == 0 else None)
        t.add(mmsi, m(10 + 5 * rows + 10), PORT, 3.0)
    arr, st = run(spark, t)
    assert arr["a10"]["anchor_stops"] == 1
    assert arr["a11"]["stops"] == 1 and arr["a11"]["anchor_stops"] == 0
    share = {s["mmsi"]: s["anchored_share"] for s in st}
    assert share["a10"] == pytest.approx(0.1) and share["a11"] == pytest.approx(1 / 11)


def test_stationary_means_slow_or_at_anchor(spark):
    t = Track().add("s", m(0), SEA, 10.0)
    for i in range(7):  # moving fast but reporting At anchor: still counts as stationary
        t.add("s", m(10 + 10 * i), ANCHORAGE, 4.0, "At anchor")
    t.add("s", m(90), PORT, 3.0)
    t.add("n", m(0), SEA, 10.0)
    for i in range(7):  # sog exactly 0.5 and not anchored: moving
        t.add("n", m(10 + 10 * i), ANCHORAGE, 0.5, None)
    t.add("n", m(90), PORT, 3.0)
    arr, _ = run(spark, t)
    assert arr["s"]["stops"] == 1 and arr["n"]["stops"] == 0


def test_stop_cut_at_start(spark):
    t = Track()
    t.still("c", m(0), 60)  # the first row of the block is already waiting
    t.add("c", m(70), PORT, 3.0)
    _, st = run(spark, t)
    assert st[0]["cut_at_start"]


def test_ship_type_is_the_mode_over_all_days_ties_alphabetical(spark):
    t = Track()
    types = ["Tanker", "Cargo", "Undefined", "Undefined", "Undefined", None, "Tanker", "Cargo"]
    for i, typ in enumerate(types):  # Cargo 2, Tanker 2 -> Cargo; Undefined and null ignored
        t.add("y", m(i), SEA, 10.0, shiptype=typ)
    t.add("y", m(20), PORT, 3.0, shiptype=None)
    t.add("u", m(0), SEA, 10.0, shiptype="Undefined").add("u", m(9), PORT, 3.0, shiptype=None)
    # a later block of ship y: its type comes from all loaded days, not from this block
    later = datetime(2025, 11, 12, 6)
    t.add("y", later, SEA, 10.0, shiptype=None).add("y", later + timedelta(minutes=9), PORT, 3.0)
    t.rows[-1]["shiptype"] = None
    arrivals, _, _ = r1.detect(r1.class_a_thinned(t.df(spark), ["2025-11-08", "2025-11-12"]))
    got = {(r["mmsi"], r["block"]): r["type"] for r in arrivals.collect()}
    assert got == {("y", 0): "Cargo", ("y", 1): "Cargo", ("u", 0): "Undefined"}


def test_blocks_follow_consecutive_days(spark):
    t = Track()
    for d in (8, 9, 12):
        base = datetime(2025, 11, d, 6)
        t.add(f"b{d}", base, SEA, 10.0).add(f"b{d}", base + timedelta(minutes=10), PORT, 3.0)
    arr, _ = run(spark, t, days=["2025-11-08", "2025-11-09", "2025-11-12"])
    assert (arr["b8"]["block"], arr["b9"]["block"], arr["b12"]["block"]) == (0, 0, 1)


def test_only_listed_days_are_used(spark):
    t = Track().add("d", m(0), SEA, 10.0).add("d", m(10), PORT, 3.0)
    arr, _ = run(spark, t, days=["2025-11-09"])
    assert arr == {}


@pytest.mark.parametrize(
    ("typ", "length", "large"),
    [
        ("Cargo", 50.0, True),
        ("Tanker", None, True),
        ("Dredging", 90.0, True),
        ("Dredging", 89.9, False),
        ("Tug", 120.0, False),
        ("Passenger", 200.0, False),
        ("Undefined", None, False),
    ],
)
def test_large_ship(spark, typ, length, large):
    df = spark.createDataFrame([(typ, length)], "type string, length double")
    got = df.select(r1.large_ship(F.col("type"), F.col("length")).alias("x")).first()["x"]
    assert got is large


# ---------------------------------------------------------------- ingestion
HEADER = ",".join(r1.CSV_COLUMNS)


def _line(ts="2025-11-08 06:00:00", mmsi="1", lat="55.42", lon="8.12", nav="At anchor", eta=""):
    vals = dict.fromkeys(r1.CSV_COLUMNS, "")
    vals.update(ts=ts, mobile="Class A", mmsi=mmsi, lat=lat, lon=lon, nav=nav, sog="0.1", eta=eta)
    return ",".join(vals[c] for c in r1.CSV_COLUMNS)


def _lines(spark, values):
    rows = [("2025-11-08", i, v) for i, v in enumerate([HEADER, *values])]
    return spark.createDataFrame(rows, "day string, idx long, value string")


def test_parse_lines_rejects_with_reasons_and_counts_add_up(spark):
    values = [
        _line(),
        _line(lat=""),
        _line(ts="not a time", lon=""),
        "",  # blank line: skipped, like pandas
        _line(ts="", lat="abc"),
        _line(mmsi="NA"),  # pandas reads "NA" as NaN
    ]
    typed = r1.parse_lines(_lines(spark, values))
    good, bad = r1.split_rejects(typed)
    reasons = sorted(r["reject_reason"] for r in bad.collect())
    assert reasons == ["lat_missing", "ts_missing;lat_unparsable", "ts_unparsable;lon_missing"]
    assert typed.count() == good.count() + bad.count() == 5
    rows = {r["line_no"]: r for r in good.collect()}
    assert set(rows) == {0, 5}  # line_no counts data records, the blank line included
    assert rows[0]["ts"] == datetime(2025, 11, 8, 6) and rows[0]["lat"] == 55.42
    assert rows[5]["mmsi"] is None


# ---------------------------------------------------------------- ETA reported at entry
def test_eta_at_entry_uses_quarantined_rows_and_keeps_arrivals_without_eta(spark):
    arrival = datetime(2025, 11, 10, 12, 0)

    def ts(h):
        return (arrival + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S")

    values = [
        _line(ts=ts(-100), mmsi="e1", eta="09/11/2025 00:00:00"),  # older than 3 days
        _line(ts=ts(-2), mmsi="e1", lat="", eta="10/11/2025 11:00:00"),  # no lat: quarantined
        _line(ts=ts(1), mmsi="e1", eta="10/11/2025 23:00:00"),  # after the arrival
        _line(ts=ts(-72), mmsi="e2", eta="25/11/2025 12:00:00"),  # exactly 3 days, >7 d off
        _line(ts=ts(-1), mmsi="e3", eta="00/00/0000 24:60:00"),  # unparsable ETA
    ]
    typed = r1.parse_lines(_lines(spark, values))
    arrivals = spark.createDataFrame(
        [(0, mmsi, arrival, True) for mmsi in ("e1", "e2", "e3", "e4")],
        "block int, mmsi string, arrival timestamp, large boolean",
    )
    out = {r["mmsi"]: r for r in r1.eta_at_entry(arrivals, r1.eta_messages(typed, DAYS)).collect()}
    assert set(out) == {"e1", "e2", "e3", "e4"}
    assert out["e1"]["eta"] == datetime(2025, 11, 10, 11) and out["e1"]["diff_h"] == 1.0
    assert out["e1"]["kept_7d"]
    assert out["e2"]["eta"] == datetime(2025, 11, 25, 12) and not out["e2"]["kept_7d"]
    assert out["e3"]["eta"] is None and out["e4"]["eta"] is None


def test_ingest_replaces_selected_days_completely(spark, tmp_path):
    """A day that goes from rejects to no rejects keeps no stale rows in either zone; days that
    were not selected keep theirs."""
    std_path, quar_path = f"file://{tmp_path}/std", f"file://{tmp_path}/quar"

    def lines(day, values):
        rows = [(day, i, v) for i, v in enumerate([HEADER, *values])]
        return spark.createDataFrame(rows, "day string, idx long, value string")

    def ingest(parts, days):
        frame = parts[0]
        for part in parts[1:]:
            frame = frame.unionByName(part)
        good, bad = r1.split_rejects(r1.parse_lines(frame))
        r1.replace_day_partitions(spark, good, std_path, days)
        r1.replace_day_partitions(spark, bad, quar_path, days)

    def counts(path, schema):
        df = r1.read_lake(spark, path, schema)
        return {r["day"]: r["count"] for r in df.groupBy("day").count().collect()}

    a, b = "2025-11-08", "2025-11-09"
    first_a = [_line(), _line(lat=""), _line(mmsi="2")]  # 2 good, 1 reject
    ingest([lines(a, first_a), lines(b, [_line(ts="2025-11-09 01:00:00")])], [a, b])
    assert counts(std_path, r1.STANDARDIZED_SCHEMA) == {a: 2, b: 1}
    assert counts(quar_path, r1.QUARANTINE_SCHEMA) == {a: 1}

    ingest([lines(a, [_line(mmsi="3")])], [a])  # day a again: 1 good row, no rejects
    assert counts(std_path, r1.STANDARDIZED_SCHEMA) == {a: 1, b: 1}
    assert counts(quar_path, r1.QUARANTINE_SCHEMA) == {}
    kept = r1.read_lake(spark, std_path).where(F.col("day") == a).first()
    assert kept["mmsi"] == "3"
