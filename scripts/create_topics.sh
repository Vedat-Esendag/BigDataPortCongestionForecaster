#!/usr/bin/env bash
# Create all Kafka topics (idempotent). Runs kafka-topics inside the kafka container, so no
# Kafka CLI is needed on the host. Start the stack first: make up-core
#
# Keys: every AIS topic and ship.prediction are keyed by MMSI, port.outlook, alerts, weather.*,
# analytics.weather_delay and eval.metrics are keyed by port. Keys are plain strings.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f infra/docker-compose.yml --profile '*')
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:29092}"

# name:partitions   (single broker, so replication factor is 1)
TOPICS=(
  ais.position:3
  ais.static:3
  ais.history.raw:3
  ais.history.position:3
  ais.history.static:3
  weather.forecast:1
  weather.observation:1
  ship.prediction:3
  port.outlook:1
  alerts:1
  analytics.weather_delay:1
  eval.metrics:1
)

for entry in "${TOPICS[@]}"; do
  name="${entry%%:*}"
  partitions="${entry##*:}"
  "${COMPOSE[@]}" exec -T kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo
"${COMPOSE[@]}" exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list
