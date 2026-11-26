#!/usr/bin/env bash
# Checks that ksqlDB keeps its persistent queries when its pod restarts, and that they carry on
# from where they were (no row lost, no row processed twice). ksqlDB keeps its queries in the
# command topic in Kafka, so the pod itself holds no state.
#   infra/k8s/test-ksqldb-restart.sh                    the school cluster   (or: make k8s-test-ksqldb)
#   PF_CLUSTER=local infra/k8s/test-ksqldb-restart.sh   the local kind one   (or: make local-test-ksqldb)
#
# It sends two test rows (MMSI 999999999) to ais.history.raw, one before and one after the
# restart, and checks that each comes out of the history split exactly once. The test rows stay
# in the ais.history.* topics; filter MMSI 999999999 out downstream if it matters. ksqlDB is
# down for about 30 seconds, so tell the group first on the school cluster.
set -euo pipefail
cd "$(dirname "$0")/../.."

case "${PF_CLUSTER:-school}" in
  school) source infra/k8s/cluster/env.sh >/dev/null || exit 1 ;;
  local) source infra/k8s/kind/env.sh >/dev/null || exit 1 ;;
  *) echo "PF_CLUSTER must be school or local" >&2; exit 2 ;;
esac

TEST_MMSI=999999999

ksqldb_pod() { kubectl get pod -l app.kubernetes.io/name=ksqldb -o name | head -n 1; }
registry_pod() { kubectl get pod -l app.kubernetes.io/name=schema-registry -o name | head -n 1; }

# The Confluent images have no curl, so the HTTP request is sent from bash inside the pod.
show_queries() {
  kubectl exec "$(ksqldb_pod)" -c ksqldb -- bash -c '
    body="{\"ksql\":\"SHOW QUERIES;\"}"
    exec 3<>/dev/tcp/localhost/8088
    printf "POST /ksql HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/vnd.ksql.v1+json\r\nContent-Length: ${#body}\r\n\r\n%s" "$body" >&3
    cat <&3'
}

# "<query id> <state>" per line, sorted.
query_states() {
  show_queries | python3 -c '
import json, sys
body = sys.stdin.read().split("\r\n\r\n", 1)[-1]
for entry in json.loads(body):
    for query in entry.get("queries", []):
        print(query["id"], query.get("state", "UNKNOWN"))' | sort
}

end_offset() { # topic: sum of the end offsets of all its partitions
  kubectl exec kafka-0 -c kafka -- kafka-get-offsets --bootstrap-server localhost:9092 --topic "$1" |
    awk -F: '{ sum += $3 } END { print sum + 0 }'
}

send_row() { # label written into the name field
  local id row
  id="$(kubectl exec "$(registry_pod)" -c schema-registry -- bash -c '
    exec 3<>/dev/tcp/localhost/8081
    printf "GET /subjects/ais.history.raw-value/versions/latest HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3
    cat <&3' | grep -o '"id":[0-9]*' | head -n 1 | cut -d: -f2)"
  row="$TEST_MMSI|{\"timestamp\":{\"string\":\"$(date -u +%d/%m/%Y\ %H:%M:%S)\"},\"type_of_mobile\":null,\"mmsi\":{\"string\":\"$TEST_MMSI\"},\"latitude\":{\"string\":\"55.47\"},\"longitude\":{\"string\":\"8.41\"},\"navigational_status\":null,\"rot\":null,\"sog\":{\"string\":\"0.0\"},\"cog\":null,\"heading\":null,\"imo\":null,\"callsign\":null,\"name\":{\"string\":\"$1\"},\"ship_type\":null,\"cargo_type\":null,\"width\":null,\"length\":null,\"position_fixing_device\":null,\"draught\":null,\"destination\":{\"string\":\"RESTART TEST\"},\"eta\":null,\"data_source_type\":null,\"a\":null,\"b\":null,\"c\":null,\"d\":null}"
  kubectl exec -i "$(registry_pod)" -c schema-registry -- kafka-avro-console-producer \
    --bootstrap-server kafka:9092 --topic ais.history.raw \
    --property schema.registry.url=http://localhost:8081 --property value.schema.id="$id" \
    --property parse.key=true --property key.separator='|' \
    --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
    <<<"$row" >/dev/null 2>&1
}

expect_one_more() { # topic, offset before, what to call this step
  local now
  for _ in $(seq 20); do
    now="$(end_offset "$1")"
    (( now >= $2 + 1 )) && break
    sleep 3
  done
  (( now == $2 + 1 )) || { echo "FAIL: $1 went from $2 to $now after $3, expected exactly +1"; exit 1; }
  echo "  OK: $1 $2 -> $now after $3"
}

echo "1. Queries before the restart"
before="$(query_states)"
[[ -n "$before" ]] || { echo "FAIL: no persistent queries. Run make local-up (or the ksql step) first."; exit 1; }
sed 's/^/  /' <<<"$before"
grep -qv ' RUNNING$' <<<"$before" && { echo "FAIL: not every query is RUNNING before the test"; exit 1; }

echo "2. A test row goes through the history split"
offset="$(end_offset ais.history.position)"
send_row "BEFORE RESTART"
expect_one_more ais.history.position "$offset" "a row before the restart"

echo "3. Delete the ksqlDB pod"
old="$(ksqldb_pod)"
start=$(date +%s)
kubectl delete "$old" --wait=true >/dev/null
kubectl rollout status deploy/ksqldb --timeout=300s >/dev/null
echo "  new pod $(ksqldb_pod) ready after $(($(date +%s) - start)) s"

echo "4. The same queries come back by themselves"
for _ in $(seq 40); do
  after="$(query_states 2>/dev/null || true)"
  [[ "$after" == "$before" ]] && break
  sleep 3
done
[[ "$after" == "$before" ]] || { printf 'FAIL: queries after the restart differ:\n%s\n' "$after"; exit 1; }
echo "  OK: $(wc -l <<<"$after") queries RUNNING again"

echo "5. A row after the restart goes through, and the old one is not processed again"
offset="$(end_offset ais.history.position)"
send_row "AFTER RESTART"
expect_one_more ais.history.position "$offset" "a row after the restart"

echo
echo "ksqlDB keeps its queries across a pod restart, with no row lost or repeated."
