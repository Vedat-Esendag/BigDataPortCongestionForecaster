"""Redis holds the latest state: one JSON document per ship and per port.

Keys written by the Redis sink connector (connect/redis-sink.json):
    ship.prediction:<mmsi>    latest ShipPrediction
    port.outlook:<port>       latest PortOutlook slot for the port
"""

from __future__ import annotations

from redis.asyncio import Redis

from api.settings import Settings

SHIP_KEY_PREFIX = "ship.prediction:"
OUTLOOK_KEY_PREFIX = "port.outlook:"


def create_redis(settings: Settings) -> Redis:
    """Create the client. It connects lazily, so this works while Redis is still down."""
    return Redis.from_url(settings.redis_url, decode_responses=True)
