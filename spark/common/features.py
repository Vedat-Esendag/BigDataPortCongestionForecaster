"""F6: build the feature table used for training (batch) and for live prediction (streaming).

The same `build_features` runs in both modes, so a model sees at prediction time exactly what it
was trained on. STUB: it only carries the columns the placeholder predictor needs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns the MLlib model reads. TODO(F6): extend together with build_features.
# Planned: distance_to_port_nm, sog_mean_30min, hours_to_broadcast_eta, ship_type, length_m,
# draught_m, wind_speed_kn, wave_height_m, hour_of_day, day_of_week, queue_size_at_arrival.
FEATURE_COLUMNS: list[str] = []


def parse_weather_forecast(forecast: DataFrame) -> DataFrame:
    """Unpack the raw JSON `payload` of weather.forecast into one row per port and hour.

    TODO(F6): parse the Open-Meteo columnar hourly arrays (time[], wind_speed_10m[], ...) with
              from_json and arrays_zip + explode. Marine and wind products arrive separately.
    """
    raise NotImplementedError("TODO(F6): parse_weather_forecast")


def build_features(
    static: DataFrame,
    positions: DataFrame | None = None,
    events: DataFrame | None = None,
    weather: DataFrame | None = None,
) -> DataFrame:
    """Combine static data, positions, port events and weather into one row per ship and time.

    Input: `static` must already have a `port` column (see port_events.assign_destination_port).
    Output: at least the columns of `common.schemas.FEATURE_BASE_SCHEMA`.

    TODO(F6): join the latest position of each ship (lat, lon, sog, distance to port).
    TODO(F6): join port events (time already spent in approach or anchorage).
    TODO(F6): join the weather forecast for the expected arrival hour, with the DMI observation
              as ground truth when training.
    TODO(F6): queue features: how many ships are ahead of this one at the berth.
    """
    return static.select(
        "mmsi",
        "port",
        F.col("eta_broadcast").alias("broadcast_eta"),
        "event_time",
        F.lit(None).cast("double").alias("lat"),
        F.lit(None).cast("double").alias("lon"),
    )
