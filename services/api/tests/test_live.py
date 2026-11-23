"""The pieces of the read path and the live push, one at a time, without Redis or Cassandra."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from conftest import HOUR, MINUTE, T0, FakeLiveStore, outlook_doc, ship_doc

from api.clients.cassandra_client import _utc
from api.clients.redis_client import drop_departed, parse_key
from api.errors import INVALID_DOCUMENTS, parse_documents
from api.live import ConnectionManager, LiveFeed
from api.models import Ship


class RecordingSocket:
    """A WebSocket that remembers what was sent to it, or fails if told to."""

    def __init__(self, fail: bool = False) -> None:
        self.sent: list[str] = []
        self.fail = fail

    async def accept(self) -> None:
        pass

    async def send_text(self, text: str) -> None:
        if self.fail:
            raise ConnectionResetError("browser went away")
        self.sent.append(text)


def ships(*docs: dict) -> list[Ship]:
    return parse_documents(Ship, list(docs), store="test")


# ---------------------------------------------------------------- keys and models


def test_parse_key_tells_ships_from_outlooks():
    assert parse_key("ship.prediction:219000123") == ("ship", "219000123")
    assert parse_key("port.outlook:dkebj") == ("outlook", "dkebj")
    assert parse_key("port.outlook:dkebj:1794841200000") == ("outlook", "dkebj")
    assert parse_key("something.else:1") is None


def test_null_reason_codes_from_cassandra_become_an_empty_list():
    [ship] = ships(ship_doc(1, reason_codes=None))

    assert ship.reason_codes == []


def test_a_broken_record_is_skipped_and_counted():
    before = INVALID_DOCUMENTS.labels(store="test")._value.get()

    parsed = ships(ship_doc(1), {"mmsi": "not a number"})

    assert [s.mmsi for s in parsed] == [1]
    assert INVALID_DOCUMENTS.labels(store="test")._value.get() == before + 1


def test_cassandra_timestamps_are_marked_utc():
    row = _utc({"event_time": datetime(2026, 11, 16, 13, 0), "port": "dkebj"})

    assert row["event_time"].tzinfo is UTC
    assert row["port"] == "dkebj"


# ---------------------------------------------------------------- departed ships


def test_ships_far_behind_the_newest_one_are_dropped():
    fresh = ship_doc(1, processed_at=T0)
    recent = ship_doc(2, processed_at=T0 - 30 * MINUTE)
    departed = ship_doc(3, processed_at=T0 - 7 * HOUR)

    kept = drop_departed(ships(fresh, recent, departed), timedelta(hours=6))

    assert [s.mmsi for s in kept] == [1, 2]


def test_when_everything_is_old_everything_stays_visible():
    # The whole pipeline stopped a day ago: show the last known state, don't empty the map.
    yesterday = T0 - 24 * HOUR
    stale = ships(ship_doc(1, processed_at=yesterday), ship_doc(2, processed_at=yesterday))

    assert len(drop_departed(stale, timedelta(hours=6))) == 2


# ---------------------------------------------------------------- broadcasting


def test_broadcast_respects_each_clients_port():
    async def scenario():
        manager = ConnectionManager()
        everything, esbjerg, elsewhere = RecordingSocket(), RecordingSocket(), RecordingSocket()
        for socket in (everything, esbjerg, elsewhere):
            await manager.connect(socket)
        manager.subscribe(esbjerg, "dkebj")
        manager.subscribe(elsewhere, "dkaar")
        await manager.broadcast({"type": "ship"}, port="dkebj")
        return everything, esbjerg, elsewhere

    everything, esbjerg, elsewhere = asyncio.run(scenario())

    assert len(everything.sent) == 1
    assert len(esbjerg.sent) == 1
    assert elsewhere.sent == []


def test_a_failing_client_is_dropped_and_the_others_still_get_the_message():
    async def scenario():
        manager = ConnectionManager()
        good, broken = RecordingSocket(), RecordingSocket(fail=True)
        await manager.connect(good)
        await manager.connect(broken)
        await manager.broadcast({"type": "ship"}, port="dkebj")
        await manager.broadcast({"type": "ship"}, port="dkebj")
        return good, manager

    good, manager = asyncio.run(scenario())

    assert len(good.sent) == 2
    assert list(manager._sockets) == [good]


# ---------------------------------------------------------------- the change handler


class StoreWithDocuments(FakeLiveStore):
    def __init__(self, documents: dict[str, dict]) -> None:
        super().__init__()
        self.documents = documents

    async def get_document(self, key: str):
        return self.documents.get(key)


def run_change(documents: dict[str, dict], key: str) -> list[str]:
    async def scenario():
        manager = ConnectionManager()
        socket = RecordingSocket()
        await manager.connect(socket)
        await LiveFeed(StoreWithDocuments(documents), manager).handle_change(key)
        return socket.sent

    return asyncio.run(scenario())


def test_a_changed_ship_key_is_pushed_as_a_ship_message():
    sent = run_change({"ship.prediction:7": ship_doc(7)}, "ship.prediction:7")

    assert len(sent) == 1
    assert '"type": "ship"' in sent[0]
    assert '"mmsi": 7' in sent[0]


def test_a_changed_outlook_key_is_pushed_with_its_port():
    sent = run_change({"port.outlook:dkebj": outlook_doc()}, "port.outlook:dkebj")

    assert '"type": "outlook"' in sent[0]
    assert '"port": "dkebj"' in sent[0]


def test_deleted_keys_and_broken_documents_are_not_pushed():
    assert run_change({}, "ship.prediction:7") == []
    assert run_change({"ship.prediction:7": {"mmsi": 7}}, "ship.prediction:7") == []
