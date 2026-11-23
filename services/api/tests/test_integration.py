"""The API against a real Redis and a real Cassandra.

Skipped unless both answer. To run them locally:

    make up-core            # or at least the storage profile: Redis, Cassandra, cassandra-init
    cd services/api && uv run pytest tests/test_integration.py

REDIS_URL and CASSANDRA_CONTACT_POINTS point the tests elsewhere (defaults: localhost).
The tests write their own keys and rows, with MMSIs no real ship has, and remove the Redis
keys again afterwards.
"""

from __future__ import annotations

import socket
import threading
import time
from datetime import UTC, datetime
from urllib.parse import urlparse

import pytest
import redis
from conftest import HOUR, T0, outlook_doc, ship_doc
from fastapi.testclient import TestClient

from api.live import LIVE_FEED_UP
from api.main import create_app
from api.settings import load_settings

SETTINGS = load_settings()
# MMSIs starting with 0 aren't valid for ships, so these never clash with real data.
SHIP_HERE, SHIP_ELSEWHERE, SHIP_DEPARTED, SHIP_PUSHED = 1001, 1002, 1003, 1004
TEST_PORT = "dkebj"


def reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


_redis_address = urlparse(SETTINGS.redis_url)
redis_up = reachable(_redis_address.hostname or "localhost", _redis_address.port or 6379)
cassandra_up = reachable(SETTINGS.cassandra_contact_points[0], SETTINGS.cassandra_port)
needs_redis = pytest.mark.skipif(not redis_up, reason="Redis is not running")
needs_cassandra = pytest.mark.skipif(not cassandra_up, reason="Cassandra is not running")


@pytest.fixture
def redis_writer():
    """A plain Redis client that plays the role of the Redis sink connector."""
    client = redis.Redis.from_url(SETTINGS.redis_url, decode_responses=True)
    yield client
    for mmsi in (SHIP_HERE, SHIP_ELSEWHERE, SHIP_DEPARTED, SHIP_PUSHED):
        client.delete(f"ship.prediction:{mmsi}")
    client.delete(f"port.outlook:{TEST_PORT}")
    client.close()


@needs_redis
def test_ships_and_outlook_are_read_from_redis(redis_writer):
    redis_writer.json().set(f"ship.prediction:{SHIP_HERE}", "$", ship_doc(SHIP_HERE))
    redis_writer.json().set(
        f"ship.prediction:{SHIP_ELSEWHERE}", "$", ship_doc(SHIP_ELSEWHERE, port="dkaar")
    )
    redis_writer.json().set(
        f"ship.prediction:{SHIP_DEPARTED}", "$", ship_doc(SHIP_DEPARTED, processed_at=T0 - 7 * HOUR)
    )
    redis_writer.json().set(f"port.outlook:{TEST_PORT}", "$", outlook_doc())

    with TestClient(create_app()) as client:
        ships = client.get("/ships", params={"port": TEST_PORT}).json()
        one = client.get(f"/ships/{SHIP_HERE}").json()
        outlook = client.get("/outlook", params={"port": TEST_PORT}).json()
        ready = client.get("/ready")

    mmsis = [s["mmsi"] for s in ships]
    assert SHIP_HERE in mmsis
    assert SHIP_ELSEWHERE not in mmsis  # other port
    assert SHIP_DEPARTED not in mmsis  # 7 hours behind the newest ship
    assert one["predicted_eta"] == "2026-11-16T16:00:00Z"
    assert outlook["slots"][0]["expected_arrivals"] == 4
    assert ready.status_code == 200


@needs_redis
def test_a_write_to_redis_is_pushed_to_a_websocket(redis_writer):
    """The whole live path: a sink-style JSON.SET reaches a subscribed dashboard."""
    with TestClient(create_app()) as client, client.websocket_connect("/ws") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_json({"type": "subscribe", "port": TEST_PORT})
        assert ws.receive_json()["type"] == "subscribed"

        # Wait until this app's live feed has subscribed to Redis, or the write could be missed.
        deadline = time.monotonic() + 10
        while LIVE_FEED_UP._value.get() != 1 and time.monotonic() < deadline:
            time.sleep(0.1)
        assert LIVE_FEED_UP._value.get() == 1, "live feed never connected to Redis"

        # Write the same document a few times in the background, so the test doesn't depend on
        # the exact moment the subscription became active.
        stop = threading.Event()

        def keep_writing():
            key = f"ship.prediction:{SHIP_PUSHED}"
            while not stop.is_set():
                redis_writer.json().set(key, "$", ship_doc(SHIP_PUSHED))
                stop.wait(0.2)

        writer = threading.Thread(target=keep_writing, daemon=True)
        writer.start()
        try:
            message = ws.receive_json()
        finally:
            stop.set()
            writer.join()

    assert message["type"] == "ship"
    assert message["ship"]["mmsi"] == SHIP_PUSHED
    assert message["ship"]["port"] == TEST_PORT


@needs_cassandra
def test_history_is_read_from_cassandra():
    from cassandra.cluster import Cluster

    cluster = Cluster(list(SETTINGS.cassandra_contact_points), port=SETTINGS.cassandra_port)
    try:
        session = cluster.connect()
        if "portcongestion" not in cluster.metadata.keyspaces:
            pytest.skip("keyspace portcongestion missing: run cassandra/schema.cql first")
        session.set_keyspace("portcongestion")
        day = "1999-01-01"  # a day no real data has
        for minute, codes in ((0, ["high_wind"]), (5, [])):
            session.execute(
                "INSERT INTO ship_prediction_by_port_day (port, day, event_time, mmsi, "
                "predicted_eta, eta_lower, eta_upper, model_version, reason_codes, lat, lon, "
                "processing_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    TEST_PORT,
                    day,
                    datetime(1999, 1, 1, 12, minute, tzinfo=UTC),
                    SHIP_HERE,
                    datetime(1999, 1, 1, 15, 0, tzinfo=UTC),
                    datetime(1999, 1, 1, 14, 30, tzinfo=UTC),
                    datetime(1999, 1, 1, 15, 30, tzinfo=UTC),
                    "v3",
                    codes,
                    55.4,
                    8.1,
                    datetime(1999, 1, 1, 12, minute, 5, tzinfo=UTC),
                ),
            )
    finally:
        cluster.shutdown()

    with TestClient(create_app()) as client:
        rows = client.get("/history/ships", params={"port": TEST_PORT, "day": day}).json()
        one = client.get("/history/ships", params={"port": TEST_PORT, "day": day, "limit": 1})

    assert [r["event_time"] for r in rows] == ["1999-01-01T12:05:00Z", "1999-01-01T12:00:00Z"]
    assert rows[0]["reason_codes"] == []  # stored as null, returned as an empty list
    assert len(one.json()) == 1
