"""Cassandra holds history: every prediction and outlook, by (port, day) (docs/storage.md).

Tables are defined in cassandra/schema.cql and filled by the Cassandra sink connector.
The API only reads.

Three things about the Python driver shape this file:

* It is not asyncio-based. Queries are sent with `execute_async`, which returns the driver's own
  future, and `_await` turns that into an asyncio future. So a slow query never blocks the event
  loop, and the WebSocket push keeps running while history loads.
* Connecting (and preparing statements) blocks for up to a few seconds, so that runs in a worker
  thread. The connection is made on first use, so the API starts, and its tests run, without
  Cassandra.
* Timestamps come back as naive datetimes that are really UTC. They are marked as UTC here.
  Otherwise they reach the dashboard without a time zone and the browser reads them as local
  time, one hour off in Denmark in winter.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime
from typing import Any

from api.errors import StoreUnavailable, parse_documents
from api.models import OutlookSlot, Ship
from api.settings import Settings

log = logging.getLogger(__name__)

# Rows of one partition (one port, one day), newest first thanks to the clustering order.
SHIP_HISTORY_CQL = "SELECT * FROM ship_prediction_by_port_day WHERE port = ? AND day = ? LIMIT ?"
OUTLOOK_HISTORY_CQL = "SELECT * FROM port_outlook_by_port_day WHERE port = ? AND day = ? LIMIT ?"


class CassandraClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cluster: Any = None
        self._session: Any = None
        self._prepared: dict[str, Any] = {}
        self._lock = threading.Lock()  # two first requests at once must not open two connections

    # ------------------------------------------------------------------ connection

    def _connect(self) -> Any:
        """Return the session, connecting first if needed. Blocking: call it from a thread."""
        with self._lock:
            if self._session is not None:
                return self._session
            from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
            from cassandra.policies import DCAwareRoundRobinPolicy
            from cassandra.query import dict_factory

            profile = ExecutionProfile(
                # The same data center name the sink connector uses. Naming it avoids a driver
                # warning and keeps queries in our data center if there is ever more than one.
                load_balancing_policy=DCAwareRoundRobinPolicy(
                    local_dc=self._settings.cassandra_local_dc
                ),
                row_factory=dict_factory,  # rows as {column: value}, ready for pydantic
                request_timeout=10,
            )
            cluster = Cluster(
                list(self._settings.cassandra_contact_points),
                port=self._settings.cassandra_port,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                connect_timeout=5,
            )
            try:
                session = cluster.connect(self._settings.cassandra_keyspace)
            except Exception:
                cluster.shutdown()  # don't leak its threads; the next request tries again
                raise
            self._cluster, self._session = cluster, session
            log.info("Connected to Cassandra, keyspace %s", self._settings.cassandra_keyspace)
            return session

    def _prepare(self, cql: str) -> Any:
        """Prepared statement for `cql`, prepared once per process. Blocking."""
        if cql not in self._prepared:
            self._prepared[cql] = self._connect().prepare(cql)
        return self._prepared[cql]

    def try_connect(self) -> None:
        """Connect now if possible, and stay quiet if not. Run at startup, in a thread."""
        try:
            self._connect()
        except Exception as error:
            log.warning("Cassandra is not reachable yet (%s); will retry on first query", error)

    def is_connected(self) -> bool:
        """True if there is a session and the driver sees at least one live Cassandra node."""
        cluster = self._cluster
        return cluster is not None and any(host.is_up for host in cluster.metadata.all_hosts())

    def close(self) -> None:
        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None
            self._session = None
            self._prepared.clear()

    # ------------------------------------------------------------------ queries

    async def query(self, cql: str, *params: Any) -> list[dict[str, Any]]:
        """Run one prepared query and return its rows. Raises StoreUnavailable if it fails."""
        try:
            statement = await asyncio.to_thread(self._prepare, cql)
            rows = await _await(self._session.execute_async(statement, params))
        except Exception as error:
            # Covers "no host available", timeouts and a missing keyspace: for the dashboard
            # they all mean "history can't be loaded right now".
            raise StoreUnavailable("cassandra", str(error)) from error
        return [_utc(row) for row in rows]

    async def ship_history(self, port: str, day: str, limit: int) -> list[Ship]:
        rows = await self.query(SHIP_HISTORY_CQL, port, day, limit)
        return parse_documents(Ship, rows, store="cassandra")

    async def outlook_history(self, port: str, day: str, limit: int) -> list[OutlookSlot]:
        rows = await self.query(OUTLOOK_HISTORY_CQL, port, day, limit)
        return parse_documents(OutlookSlot, rows, store="cassandra")


async def _await(response_future: Any) -> list[dict[str, Any]]:
    """Wait for a driver ResponseFuture from asyncio.

    The driver calls back from its own I/O thread, so the result is handed to the event loop with
    call_soon_threadsafe. Only the first page of rows is returned. That is fine because every
    query here has a LIMIT below the driver's page size (5000 rows).
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[list[dict[str, Any]]] = loop.create_future()

    def deliver(setter: Any, value: Any) -> None:
        if not future.done():  # the request may have been cancelled meanwhile
            setter(value)

    response_future.add_callbacks(
        callback=lambda rows: loop.call_soon_threadsafe(deliver, future.set_result, list(rows)),
        errback=lambda error: loop.call_soon_threadsafe(deliver, future.set_exception, error),
    )
    return await future


def _utc(row: dict[str, Any]) -> dict[str, Any]:
    """Mark every naive datetime in a row as UTC (Cassandra stores timestamps in UTC)."""
    return {
        column: value.replace(tzinfo=UTC)
        if isinstance(value, datetime) and value.tzinfo is None
        else value
        for column, value in row.items()
    }
