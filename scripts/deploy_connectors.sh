#!/usr/bin/env bash
# Validate and deploy every connector in connect/ to Kafka Connect (idempotent PUT).
#
#   scripts/deploy_connectors.sh                  validate, then deploy
#   scripts/deploy_connectors.sh --validate-only  validate against the running Connect, deploy nothing
#
# Each connector is checked with Connect's own /config/validate endpoint before it is deployed,
# so a wrong property name fails here with a clear message instead of as a failed task later.
# Secrets are never written to disk: configs reference them as ${env:NAME} and the Connect
# worker resolves them from its own environment.
set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env (if present) so connectors that need a secret can be told apart from those that do not.
# The values stay in this process; they are not written anywhere.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
VALIDATE_ONLY=0
[[ "${1:-}" == "--validate-only" ]] && VALIDATE_ONLY=1

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT
PY=(uv run --no-project --with pyyaml python scripts/render_connectors.py)

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for _ in $(seq 1 60); do
  curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$CONNECT_URL/connectors" >/dev/null || { echo "Kafka Connect is not reachable" >&2; exit 1; }

"${PY[@]}" --out "$OUT" >"$OUT/index.tsv"

failed=0
while IFS=$'\t' read -r name class file; do
  validate_out="$OUT/$name.validate.json"
  curl -fsS -X PUT -H 'Content-Type: application/json' --data @"$file" \
    "$CONNECT_URL/connector-plugins/$class/config/validate" >"$validate_out" \
    || { echo "FAIL $name: plugin $class is not installed in Connect" >&2; failed=1; continue; }

  if ! errors="$("${PY[@]}" errors "$validate_out")"; then
    echo "FAIL $name"
    echo "$errors"
    failed=1
    continue
  fi

  if [[ "$VALIDATE_ONLY" -eq 1 ]]; then
    echo "ok   $name (valid)"
  else
    curl -fsS -X PUT -H 'Content-Type: application/json' --data @"$file" \
      "$CONNECT_URL/connectors/$name/config" >/dev/null
    echo "ok   $name (deployed)"
  fi
done <"$OUT/index.tsv"

exit "$failed"
