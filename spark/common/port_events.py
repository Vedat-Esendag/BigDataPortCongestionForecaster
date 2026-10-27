"""F5: detect port events (approach, anchorage, berth) and tie ships to a destination port.

STUBS. `detect_port_events` returns no events. `assign_destination_port` is a deliberately
naive placeholder so the demo pipeline has a port to attach to a ship.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import Port
from common.schemas import PORT_EVENT_SCHEMA

EVENT_ENTER_APPROACH = "enter_approach"
EVENT_ENTER_ANCHORAGE = "enter_anchorage"
EVENT_ENTER_BERTH = "enter_berth"
EVENT_LEAVE_PORT = "leave_port"


def detect_port_events(positions: DataFrame, ports: Sequence[Port]) -> DataFrame:
    """Turn cleaned positions into port events by testing each position against the port polygons.

    Input: cleaned positions (`common.schemas.POSITION_SCHEMA`).
    Output: one row per event, columns of `common.schemas.PORT_EVENT_SCHEMA`.

    TODO(F5): point-in-polygon test against approach_zone, anchorage and berth_area of each port
              (the polygons are in config/ports.yaml, never in code).
    TODO(F5): emit an event only on a state change per (mmsi, port), which needs per-ship state
              in streaming (transformWithState) and a window function in batch.
    TODO(F5): the same function must run on live streams and on historical batches. Keep it free
              of anything that only works in one of the two modes.
    """
    return positions.sparkSession.createDataFrame([], PORT_EVENT_SCHEMA)


def assign_destination_port(static: DataFrame, ports: Sequence[Port]) -> DataFrame:
    """Add a `port` column: which configured port the ship says it is heading to.

    PLACEHOLDER: case-insensitive substring match of the crew-typed `destination` against the
    port name and UN/LOCODE. Ships that match no port get a null port.

    TODO(F5): replace with proper destination matching (typo tolerance, routes such as
              "ESBJERG>HIRTSHALS", combine with the ship's real heading and position).
    """
    port = F.lit(None).cast("string")
    destination = F.upper(F.coalesce(F.col("destination"), F.lit("")))
    for candidate in reversed(list(ports)):
        hit = destination.contains(candidate.name.upper()) | destination.contains(
            candidate.unlocode.upper()
        )
        port = F.when(hit, F.lit(candidate.id)).otherwise(port)
    return static.withColumn("port", port)
