"""F4: clean raw AIS data.

Both functions return two DataFrames: the rows that passed and the rows that were rejected,
the latter with an extra `reject_reason` column. The streaming job writes the rejected rows to
the `quarantine` zone of the data lake, never silently drops them.

STUBS: nothing is cleaned yet, every row passes. The expected behaviour is written down as
strict xfail tests in spark/tests/test_clean.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.schemas import REJECT_REASON_COLUMN


def _no_rejects(df: DataFrame) -> DataFrame:
    return df.limit(0).withColumn(REJECT_REASON_COLUMN, F.lit(None).cast("string"))


def clean_positions(
    positions: DataFrame, *, max_sog_knots: float = 50.0
) -> tuple[DataFrame, DataFrame]:
    """Clean AIS position reports.

    Input columns follow `common.schemas.POSITION_SCHEMA`
    (at least mmsi, event_time, lat, lon, sog).

    TODO(F4): reject and quarantine
      - invalid MMSI (not 9 digits, or an MID outside 201..775 for ship stations)
      - latitude outside [-90, 90] or longitude outside [-180, 180]
      - speed over ground above `max_sog_knots`, and the "not available" code 102.3
      - heading 511 and course 360 ("not available") become null instead of being rejected
    TODO(F4): drop exact duplicates on (mmsi, event_time); in streaming this needs a watermark.
    TODO(F4): reject physically impossible jumps between consecutive positions of one ship.

    Returns:
        (clean, rejected)
    """
    return positions, _no_rejects(positions)


def clean_static(static: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean AIS static and voyage data.

    Input columns follow `common.schemas.STATIC_SCHEMA`.

    TODO(F4): validate MMSI like clean_positions.
    TODO(F4): normalize `destination` (trim, upper case, strip '>' and '@' padding).
    TODO(F4): null out a broadcast ETA that is in the distant past or more than a year ahead.
    TODO(F4): treat length, beam and draught of 0 as missing.

    Returns:
        (clean, rejected)
    """
    return static, _no_rejects(static)
