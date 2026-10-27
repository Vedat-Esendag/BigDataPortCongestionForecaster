"""Prometheus metrics. Scraped from http://<host>:PRODUCER_METRICS_PORT/metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

MESSAGES_RECEIVED = Counter(
    "ais_messages_received_total", "Messages received from aisstream", ["message_type"]
)
MESSAGES_PRODUCED = Counter("ais_messages_produced_total", "Messages delivered to Kafka", ["topic"])
PRODUCE_ERRORS = Counter(
    "ais_produce_errors_total", "Messages that could not be serialized or delivered", ["stage"]
)
MAP_ERRORS = Counter("ais_map_errors_total", "Messages that could not be mapped")
RECONNECTS = Counter("ais_reconnects_total", "WebSocket reconnect attempts")
CONNECTED = Gauge("ais_websocket_connected", "1 while the aisstream WebSocket is open")
LAST_MESSAGE = Gauge("ais_last_message_timestamp_seconds", "Unix time of the last message received")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
