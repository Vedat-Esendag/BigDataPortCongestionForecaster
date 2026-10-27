"""Smoke tests: the app starts without Redis or Cassandra and every route answers."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.main import create_app


@pytest.fixture
def client():
    with TestClient(create_app()) as test_client:
        yield test_client


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ports_come_from_the_config_file(client):
    ports = client.get("/ports").json()

    assert [p["id"] for p in ports] == ["dkebj"]
    assert ports[0]["center"] == {"lat": 55.47, "lon": 8.43}


def test_stub_endpoints_return_empty_results(client):
    assert client.get("/ships", params={"port": "dkebj"}).json() == []
    assert client.get("/outlook", params={"port": "dkebj"}).json() == {"port": "dkebj", "slots": []}
    day = {"port": "dkebj", "day": "2026-10-01"}
    assert client.get("/history/ships", params=day).json() == []
    assert client.get("/history/outlook", params=day).json() == []


def test_unknown_port_is_a_404(client):
    assert client.get("/ships", params={"port": "zzzzz"}).status_code == 404
    assert client.get("/ships/219000001").status_code == 404


def test_bad_day_is_rejected(client):
    response = client.get("/history/ships", params={"port": "dkebj", "day": "yesterday"})

    assert response.status_code == 422


def test_websocket_says_hello_and_answers_ping(client):
    with client.websocket_connect("/ws") as socket:
        assert socket.receive_json() == {"type": "hello", "ports": ["dkebj"]}
        socket.send_text("ping")
        assert socket.receive_text() == "pong"


def test_metrics_are_exposed(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "api_websocket_connections" in response.text
