"""Connect to aisstream.io, subscribe to the port bounding boxes, and feed Kafka.

Reconnects with exponential backoff. An invalid API key is fatal (retrying would only hammer
the server); everything else is retried.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import time
from typing import Any

import websockets
from confluent_kafka import KafkaException
from confluent_kafka.serialization import SerializationError

from ais_producer import metrics
from ais_producer.backoff import Backoff
from ais_producer.config import Settings, load_bounding_boxes, load_settings
from ais_producer.kafka_sink import AvroSink
from ais_producer.mapping import map_message

log = logging.getLogger("ais_producer")

# Only Class A position reports and static data are used (F1).
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]
STATS_EVERY = 5000
# A connection that stayed up this long counts as healthy, so the next retry starts from the
# short delay again. Shorter sessions keep growing the delay, which stops a flapping server
# from being retried every second.
HEALTHY_AFTER_SECONDS = 30.0


class FatalError(Exception):
    """An error that retrying cannot fix, for example an invalid API key."""


def subscription(api_key: str, boxes: list[list[list[float]]]) -> dict[str, Any]:
    """The first message aisstream expects, sent within 3 seconds of connecting."""
    return {"APIKey": api_key, "BoundingBoxes": boxes, "FilterMessageTypes": MESSAGE_TYPES}


class Handler:
    """Turns raw WebSocket frames into Kafka records and keeps the metrics."""

    def __init__(self, sink: AvroSink) -> None:
        self._sink = sink
        self._handled = 0

    def handle(self, raw: str | bytes) -> None:
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            metrics.MAP_ERRORS.inc()
            return
        if "error" in envelope:
            raise FatalError(str(envelope["error"]))

        metrics.MESSAGES_RECEIVED.labels(message_type=envelope.get("MessageType", "unknown")).inc()
        metrics.LAST_MESSAGE.set_to_current_time()

        try:
            mapped = map_message(envelope)
        except (KeyError, ValueError, TypeError) as exc:
            metrics.MAP_ERRORS.inc()
            log.warning("could not map %s message: %r", envelope.get("MessageType"), exc)
            return
        if mapped is None:
            return

        try:
            self._sink.send(mapped)
        except (SerializationError, ValueError, KafkaException) as exc:
            metrics.PRODUCE_ERRORS.labels(stage="serialize").inc()
            log.warning("could not produce to %s: %r", mapped.topic, exc)
            return

        self._handled += 1
        if self._handled % STATS_EVERY == 0:
            log.info("%d messages handled", self._handled)


async def stream(
    settings: Settings,
    handler: Handler,
    stop: asyncio.Event,
    backoff: Backoff | None = None,
) -> None:
    boxes = load_bounding_boxes(settings.ports_config)
    backoff = backoff or Backoff()
    log.info("subscribing to %d bounding boxes", len(boxes))

    while not stop.is_set():
        connected_at: float | None = None
        try:
            async with websockets.connect(settings.websocket_url) as socket:
                await socket.send(json.dumps(subscription(settings.api_key, boxes)))
                connected_at = time.monotonic()
                metrics.CONNECTED.set(1)
                log.info("connected to %s", settings.websocket_url)
                async for frame in socket:
                    handler.handle(frame)
                    if stop.is_set():
                        return
        except FatalError:
            raise
        except (websockets.exceptions.WebSocketException, OSError, TimeoutError) as exc:
            log.warning("connection lost: %r", exc)
        finally:
            metrics.CONNECTED.set(0)

        if connected_at is not None and time.monotonic() - connected_at >= HEALTHY_AFTER_SECONDS:
            backoff.reset()
        metrics.RECONNECTS.inc()
        delay = backoff.next()
        log.info("reconnecting in %.1f s", delay)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


async def amain(settings: Settings) -> int:
    sink = AvroSink(settings)
    handler = Handler(sink)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stream(settings, handler, stop)
        return 0
    except FatalError as exc:
        log.error("aisstream refused the subscription: %s", exc)
        return 2
    finally:
        remaining = await asyncio.to_thread(sink.flush)
        if remaining:
            log.warning("%d messages were not delivered before shutdown", remaining)


def run() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = load_settings()
    metrics.start_metrics_server(settings.metrics_port)
    log.info("metrics on :%d/metrics", settings.metrics_port)
    return asyncio.run(amain(settings))
