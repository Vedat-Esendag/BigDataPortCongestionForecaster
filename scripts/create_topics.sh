#!/usr/bin/env bash
# Create all Kafka topics (idempotent). Runs kafka-topics inside the kafka container, so no
# Kafka CLI is needed on the host. Start the stack first: make up-core
#
# Keys: every AIS topic and ship.prediction are keyed by MMSI, port.outlook, alerts, weather.*,
# analytics.weather_delay and eval.metrics are keyed by port. Keys are plain strings.
set -euo pipefail
cd "$(dirname "$0")/.."

# How to run a command inside the broker. Default: the compose container. Kubernetes passes
# its own, for example KAFKA_EXEC="kubectl exec -i kafka-0 -c kafka --" (infra/k8s/local-up.sh).
if [[ -n "${KAFKA_EXEC:-}" ]]; then
  read -r -a KAFKA_RUN <<<"$KAFKA_EXEC"
else
  KAFKA_RUN=(docker compose -f infra/docker-compose.yml --profile '*' exec -T kafka)
fi
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
  "${KAFKA_RUN[@]}" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo
"${KAFKA_RUN[@]}" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
