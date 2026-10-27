"""F9: aggregate ship predictions into the port outlook, one row per port and time slot.

Runs on one micro-batch of predictions (a plain DataFrame inside foreachBatch).
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import Thresholds


def build_outlook(
    predictions: DataFrame, thresholds: Thresholds, now: datetime | None = None
) -> DataFrame:
    """Count expected arrivals and late ships per port and slot for the coming hours.

    Output columns are the fields of schemas/avro/port.outlook.avsc.

    TODO(F9): this only sees the ships of the current micro-batch. A real outlook needs the
              latest prediction of every ship, so keep per-ship state (transformWithState) and
              recompute the outlook from that state.
    TODO(F9): expected_waiting_ships is 0.0 until berth capacity and anchorage queues are modelled.
    TODO(F9): weight arrivals by the 70% window instead of counting at the point prediction.
    """
    now_col = F.lit(now) if now is not None else F.current_timestamp()
    horizon = F.expr(f"INTERVAL {max(thresholds.horizons_hours)} HOURS")
    late_by = F.expr(f"INTERVAL {thresholds.late_minutes} MINUTES")

    upcoming = predictions.where(
        (F.col("predicted_eta") >= now_col) & (F.col("predicted_eta") <= now_col + horizon)
    )
    slot = F.window("predicted_eta", f"{thresholds.slot_minutes} minutes")
    is_late = F.col("predicted_eta") > F.col("broadcast_eta") + late_by

    per_slot = upcoming.groupBy("port", slot.alias("slot")).agg(
        F.count(F.lit(1)).cast("int").alias("expected_arrivals"),
        F.sum(F.when(is_late, 1).otherwise(0)).cast("int").alias("late_count"),
    )
    return per_slot.select(
        "port",
        F.col("slot.start").alias("slot_start"),
        F.col("slot.end").alias("slot_end"),
        "expected_arrivals",
        "late_count",
        (F.col("expected_arrivals") >= thresholds.bunching_min_arrivals).alias("bunching_flag"),
        F.lit(0.0).alias("expected_waiting_ships"),
        now_col.alias("computed_at"),
        F.date_format(now_col, "yyyy-MM-dd").alias("day"),
    )
