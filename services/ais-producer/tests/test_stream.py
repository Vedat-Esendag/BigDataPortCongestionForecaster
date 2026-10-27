"""The producer loop against a fake aisstream server: no Kafka, no network, no API key."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import websockets
from test_mapping import position_envelope, static_envelope

from ais_producer.backoff import Backoff
from ais_producer.config import Settings, load_bounding_boxes
from ais_producer.main import FatalError, Handler, stream, subscription

REPO = Path(__file__).resolve().parents[3]
PORTS_CONFIG = REPO / "config" / "ports.yaml"


class FakeSink:
    """Stands in for AvroSink. Records what would have gone to Kafka."""

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.sent = []
        self._fail_with = fail_with

    def send(self, mapped) -> None:
        if self._fail_with:
            raise self._fail_with
        self.sent.append(mapped)


def make_settings(url: str) -> Settings:
    return Settings(
        api_key="test-key",
        bootstrap_servers="unused",
        schema_registry_url="unused",
        ports_config=PORTS_CONFIG,
        schema_dir=REPO / "schemas" / "avro",
        metrics_port=0,
        auto_register_schemas=False,
        websocket_url=url,
    )


def test_bounding_boxes_use_aisstreams_lat_lon_corner_format():
    boxes = load_bounding_boxes(PORTS_CONFIG)

    assert len(boxes) == 1
    assert boxes[0] == [[55.2, 8.0], [55.7, 8.6]]  # Esbjerg: [lat, lon] of two corners
    for (south, west), (north, east) in boxes:
        assert south < north
        assert west < east


def test_subscription_message():
    message = subscription("key", [[[1.0, 2.0], [3.0, 4.0]]])

    assert message == {
        "APIKey": "key",
        "BoundingBoxes": [[[1.0, 2.0], [3.0, 4.0]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


class TestHandler:
    def test_valid_messages_reach_the_sink(self):
        sink = FakeSink()
        handler = Handler(sink)

        handler.handle(json.dumps(position_envelope()))
        handler.handle(json.dumps(static_envelope()))

        assert [m.topic for m in sink.sent] == ["ais.position", "ais.static"]

    def test_garbage_and_unmappable_messages_are_skipped_not_fatal(self):
        sink = FakeSink()
        handler = Handler(sink)
        broken = position_envelope()
        del broken["Message"]["PositionReport"]["Longitude"]

        handler.handle("not json at all")
        handler.handle(json.dumps(broken))
        handler.handle(json.dumps({"MessageType": "BaseStationReport", "MetaData": {}}))

        assert sink.sent == []

    def test_a_serialization_failure_does_not_stop_the_stream(self):
        handler = Handler(FakeSink(fail_with=ValueError("does not match schema")))

        handler.handle(json.dumps(position_envelope()))  # must not raise

    def test_an_error_message_from_the_server_is_fatal(self):
        handler = Handler(FakeSink())

        with pytest.raises(FatalError, match="Api Key Is Not Valid"):
            handler.handle(json.dumps({"error": "Api Key Is Not Valid"}))


def test_stream_subscribes_reconnects_and_stops_on_a_rejected_key():
    """Connection 1 delivers messages and drops; connection 2 is rejected, which ends the loop."""
    subscriptions: list[dict] = []
    messages = [
        position_envelope(),
        {"MessageType": "BaseStationReport", "MetaData": {}, "Message": {}},
        static_envelope(),
    ]

    async def fake_aisstream(socket) -> None:
        subscriptions.append(json.loads(await socket.recv()))
        if len(subscriptions) == 1:
            for message in messages:
                await socket.send(json.dumps(message))
            await socket.close()
        else:
            await socket.send(json.dumps({"error": "Api Key Is Not Valid"}))
            await asyncio.sleep(0.3)  # keep the socket open long enough to be read

    sink = FakeSink()

    async def scenario() -> None:
        async with websockets.serve(fake_aisstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            settings = make_settings(f"ws://127.0.0.1:{port}")
            fast = Backoff(base=0.01, cap=0.02)
            with pytest.raises(FatalError):
                await asyncio.wait_for(
                    stream(settings, Handler(sink), asyncio.Event(), fast), timeout=15
                )

    asyncio.run(scenario())

    assert len(subscriptions) == 2  # it reconnected once after the drop
    for sub in subscriptions:  # and re-sent the subscription every time
        assert sub["APIKey"] == "test-key"
        assert len(sub["BoundingBoxes"]) == 1
    assert [m.topic for m in sink.sent] == ["ais.position", "ais.static"]
