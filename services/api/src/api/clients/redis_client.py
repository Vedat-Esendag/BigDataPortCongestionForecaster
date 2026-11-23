"""Redis holds the latest state: one JSON document per ship and per port (docs/storage.md).

The Redis sink connector (connect/redis-sink.json) writes the keys. The key is the topic name,
a colon, then the Kafka record key:

    ship.prediction:<mmsi>    the ship's latest prediction
    port.outlook:<port>       the port's latest outlook slot

Each new result overwrites the previous one, so Redis only ever answers "what is true now".
Nothing in the API writes to Redis.

Outlook keys: today the Kafka key of port.outlook is just the port, so all slots of a port share
one key and only the newest slot survives (a known gap, see docs/storage.md). The reader below
already accepts `port.outlook:<port>:<anything>` as well, so nothing here has to change when
the key gets the slot added.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from api.errors import StoreUnavailable, parse_documents
from api.models import OutlookSlot, Ship
from api.settings import Settings

SHIP_KEY_PREFIX = "ship.prediction:"
OUTLOOK_KEY_PREFIX = "port.outlook:"

# Errors that mean "Redis can't be reached", as opposed to a bug in our code.
REDIS_DOWN = (RedisConnectionError, RedisTimeoutError, OSError)


def create_redis(settings: Settings) -> Redis:
    """Create the client. It connects lazily, so this works while Redis is still down.

    health_check_interval makes an idle connection (like the live-push subscription) notice
    within 30 seconds that Redis went away, instead of waiting forever.
    """
    return Redis.from_url(settings.redis_url, decode_responses=True, health_check_interval=30)


def ship_key(mmsi: int) -> str:
    return f"{SHIP_KEY_PREFIX}{mmsi}"


def parse_key(key: str) -> tuple[str, str] | None:
    """Say what a key holds: ("ship", "<mmsi>"), ("outlook", "<port>"), or None if neither.

    >>> parse_key("ship.prediction:219000123")
    ('ship', '219000123')
    >>> parse_key("port.outlook:dkebj:1794841200000")
    ('outlook', 'dkebj')
    """
    if key.startswith(SHIP_KEY_PREFIX):
        return "ship", key.removeprefix(SHIP_KEY_PREFIX)
    if key.startswith(OUTLOOK_KEY_PREFIX):
        return "outlook", key.removeprefix(OUTLOOK_KEY_PREFIX).split(":", 1)[0]
    return None


def drop_departed(ships: list[Ship], max_age: timedelta) -> list[Ship]:
    """Hide ships whose last prediction is much older than the newest prediction of any ship.

    A ship that berthed or sailed away stops getting predictions, but its key stays in Redis.
    Without this it would stay on the map forever.

    The age is measured against the NEWEST ship, not against the clock, on purpose. If the whole
    pipeline stops (the AIS feed drops, Spark crashes), every ship stops updating at once, and
    they all stay visible with their last known state. That is the project's degraded mode: an
    old picture with its age shown is better than an empty map.
    """
    stamps = [ship.processing_time for ship in ships if ship.processing_time is not None]
    if not stamps:
        return ships
    newest = max(stamps)
    return [
        ship
        for ship in ships
        if ship.processing_time is None or newest - ship.processing_time <= max_age
    ]


class LiveStore:
    """Reads the latest state from Redis. Every method raises StoreUnavailable if Redis is down."""

    def __init__(self, redis: Redis, ship_max_age: timedelta) -> None:
        self.redis = redis
        self.ship_max_age = ship_max_age

    async def ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except REDIS_DOWN:
            return False

    async def get_document(self, key: str) -> dict[str, Any] | None:
        """One JSON document, or None if the key doesn't exist (anymore)."""
        try:
            document = await self.redis.json().get(key)
        except REDIS_DOWN as error:
            raise StoreUnavailable("redis", str(error)) from error
        return document if isinstance(document, dict) else None

    async def get_ship(self, mmsi: int) -> Ship | None:
        document = await self.get_document(ship_key(mmsi))
        if document is None:
            return None
        ships = parse_documents(Ship, [document], store="redis")
        return ships[0] if ships else None

    async def ships_for_port(self, port: str) -> list[Ship]:
        """The latest prediction of every ship heading for `port`, departed ships left out.

        How: list every ship key with SCAN, fetch them all with one JSON.MGET, keep the ones
        for this port. SCAN walks the keys in small steps, so Redis keeps serving other clients
        meanwhile. A few thousand keys take a few milliseconds. If the number of ships ever makes
        this slow, the fix is a set of MMSIs per port.
        """
        documents = await self._documents_matching(f"{SHIP_KEY_PREFIX}*")
        for_port = [doc for doc in documents if doc.get("port") == port]
        ships = parse_documents(Ship, for_port, store="redis")
        ships = drop_departed(ships, self.ship_max_age)
        return sorted(ships, key=lambda ship: ship.predicted_eta)

    async def outlook_for_port(self, port: str) -> list[OutlookSlot]:
        """Every stored outlook slot of `port`, earliest slot first."""
        documents = await self._documents_matching(
            f"{OUTLOOK_KEY_PREFIX}{port}:*", also=f"{OUTLOOK_KEY_PREFIX}{port}"
        )
        slots = parse_documents(OutlookSlot, documents, store="redis")
        return sorted(slots, key=lambda slot: slot.slot_start)

    async def _documents_matching(self, pattern: str, also: str | None = None) -> list[dict]:
        """Every JSON document whose key matches `pattern` (plus the exact key `also`)."""
        try:
            keys = [key async for key in self.redis.scan_iter(match=pattern, count=1000)]
            if also is not None:
                keys.append(also)
            if not keys:
                return []
            # With the path "$" each result is a list holding the document, or None for a key
            # that was deleted between the SCAN and this read.
            results = await self.redis.json().mget(keys, "$")
        except REDIS_DOWN as error:
            raise StoreUnavailable("redis", str(error)) from error
        return [result[0] for result in results if result and isinstance(result[0], dict)]
