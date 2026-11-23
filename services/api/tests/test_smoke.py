"""Every route answers, with fake stores in place of Redis and Cassandra (see conftest.py)."""

from __future__ import annotations

import pytest
from conftest import FakeHistory, FakeLiveStore, outlook_doc, ship_doc
from fastapi.testclient import TestClient

from api.main import create_app


def make_client(live: FakeLiveStore | None = None, history: FakeHistory | None = None):
    app = create_app()
    app.state.live = live or FakeLiveStore()
    app.state.history = history or FakeHistory()
    return TestClient(app)


@pytest.fixture
def client():
    with make_client() as test_client:
        yield test_client


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ports_come_from_the_config_file(client):
    ports = client.get("/ports").json()

    assert [p["id"] for p in ports] == ["dkebj"]
    assert ports[0]["center"] == {"lat": 55.47, "lon": 8.43}


def test_empty_stores_give_empty_results(client):
    assert client.get("/ships", params={"port": "dkebj"}).json() == []
    assert client.get("/outlook", params={"port": "dkebj"}).json() == {"port": "dkebj", "slots": []}
    day = {"port": "dkebj", "day": "2026-10-01"}
    assert client.get("/history/ships", params=day).json() == []
    assert client.get("/history/outlook", params=day).json() == []


def test_ships_and_outlook_come_from_redis():
    live = FakeLiveStore(ships=[ship_doc(219000001)], slots=[outlook_doc()])
    with make_client(live=live) as client:
        ships = client.get("/ships", params={"port": "dkebj"}).json()
        outlook = client.get("/outlook", params={"port": "dkebj"}).json()
        one = client.get("/ships/219000001")

    assert [s["mmsi"] for s in ships] == [219000001]
    # Epoch milliseconds from Redis come out as ISO timestamps in UTC.
    assert ships[0]["predicted_eta"] == "2026-11-16T16:00:00Z"
    assert ships[0]["processing_time"] == "2026-11-16T13:00:00Z"
    assert outlook["slots"][0]["expected_arrivals"] == 4
    assert one.status_code == 200


def test_history_comes_from_cassandra_with_the_requested_limit():
    history = FakeHistory(ships=[ship_doc(219000001), ship_doc(219000002)])
    with make_client(history=history) as client:
        query = {"port": "dkebj", "day": "2026-11-16", "limit": 1}
        rows = client.get("/history/ships", params=query)

    assert len(rows.json()) == 1
    assert history.calls == [("ships", "dkebj", "2026-11-16", 1)]


def test_unknown_port_and_unknown_ship_are_404(client):
    assert client.get("/ships", params={"port": "zzzzz"}).status_code == 404
    assert client.get("/ships/219000001").status_code == 404


def test_bad_day_and_bad_limit_are_rejected(client):
    bad_day = {"port": "dkebj", "day": "yesterday"}
    assert client.get("/history/ships", params=bad_day).status_code == 422
    too_many = {"port": "dkebj", "day": "2026-11-16", "limit": 100000}
    assert client.get("/history/ships", params=too_many).status_code == 422


def test_a_store_that_is_down_gives_503_not_500():
    with make_client(live=FakeLiveStore(down=True), history=FakeHistory(down=True)) as client:
        ships = client.get("/ships", params={"port": "dkebj"})
        old = client.get("/history/ships", params={"port": "dkebj", "day": "2026-11-16"})

    assert ships.status_code == 503
    assert ships.json()["store"] == "redis"
    assert old.status_code == 503
    assert old.json()["store"] == "cassandra"


def test_ready_depends_on_redis_only():
    with make_client(history=FakeHistory(down=True)) as client:
        up = client.get("/ready")
    with make_client(live=FakeLiveStore(down=True)) as client:
        down = client.get("/ready")

    assert (up.status_code, up.json()) == (200, {"redis": True, "cassandra": False})
    assert down.status_code == 503


def test_websocket_says_hello_and_answers_ping(client):
    with client.websocket_connect("/ws") as socket:
        assert socket.receive_json() == {"type": "hello", "ports": ["dkebj"]}
        socket.send_text("ping")
        assert socket.receive_text() == "pong"


def test_websocket_subscribe_to_a_port(client):
    with client.websocket_connect("/ws") as socket:
        socket.receive_json()  # hello
        socket.send_json({"type": "subscribe", "port": "dkebj"})
        assert socket.receive_json() == {"type": "subscribed", "port": "dkebj"}
        socket.send_json({"type": "subscribe", "port": "zzzzz"})
        assert socket.receive_json()["type"] == "error"


def test_metrics_are_exposed(client):
    text = client.get("/metrics").text

    assert "api_websocket_connections" in text
    assert "api_live_feed_connected" in text
    assert "api_invalid_documents_total" in text
