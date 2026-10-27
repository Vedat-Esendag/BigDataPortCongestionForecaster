"""Cassandra holds history: predictions and outlooks by (port, day), analytics, evaluation.

The session is created on first use, so the API starts (and its tests run) without Cassandra.
Tables are defined in cassandra/schema.cql.
"""

from __future__ import annotations

from typing import Any

from api.settings import Settings


class CassandraClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cluster: Any = None
        self._session: Any = None

    @property
    def session(self) -> Any:
        if self._session is None:
            from cassandra.cluster import Cluster

            self._cluster = Cluster(
                list(self._settings.cassandra_contact_points), port=self._settings.cassandra_port
            )
            self._session = self._cluster.connect(self._settings.cassandra_keyspace)
        return self._session

    def close(self) -> None:
        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None
            self._session = None
