"""F8: turn feature rows into ship.prediction rows.

PLACEHOLDER: predicted ETA equals the broadcast ETA, with a fixed window. This lets the whole
pipeline run end to end before a model exists.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DUMMY_MODEL_VERSION = "dummy"
DUMMY_WINDOW_MINUTES = 30  # placeholder half-width of the "70% window"


def predict_eta(features: DataFrame, model=None) -> DataFrame:
    """Add predicted ETA, window, model version and reason codes to the feature rows.

    Output columns are the fields of schemas/avro/ship.prediction.avsc.

    TODO(F8): when `model` is a loaded MLlib PipelineModel, predict the delay in minutes, add it
              to the broadcast ETA, and widen the window by the residual quantiles that training
              stored next to the model. Fill `reason_codes` from feature contributions
              (for example high_wind, slow_approach).
    TODO(F8): ships without a broadcast ETA need a fallback (distance divided by speed).
    """
    window = F.expr(f"INTERVAL {DUMMY_WINDOW_MINUTES} MINUTES")
    ready = features.where(F.col("broadcast_eta").isNotNull() & F.col("port").isNotNull())
    return ready.select(
        "mmsi",
        "port",
        "broadcast_eta",
        F.col("broadcast_eta").alias("predicted_eta"),
        (F.col("broadcast_eta") - window).alias("eta_lower"),
        (F.col("broadcast_eta") + window).alias("eta_upper"),
        F.lit(DUMMY_MODEL_VERSION).alias("model_version"),
        F.array(F.lit("dummy_predictor")).alias("reason_codes"),
        "lat",
        "lon",
        "event_time",
        F.current_timestamp().alias("processing_time"),
        F.date_format("event_time", "yyyy-MM-dd").alias("day"),
    )
