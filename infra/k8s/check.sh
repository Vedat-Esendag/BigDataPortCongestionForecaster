#!/usr/bin/env bash
# Smoke test: is every service up and answering? Read-only, safe to run any time.
#   infra/k8s/check.sh            (or: make k8s-check)
set -uo pipefail
cd "$(dirname "$0")/../.."

source infra/k8s/cluster/env.sh >/dev/null || exit 1

echo "Waiting up to 5 minutes for every pod to be ready..."
kubectl wait pod -l app.kubernetes.io/part-of=port-forecaster --for=condition=Ready --timeout=300s >/dev/null ||
  echo "Not every pod is ready; the checks below show which service is affected."
echo

failed=0
check() { # name, then the command that must succeed
  local name="$1"
  shift
  if out="$("$@" 2>&1)"; then
    printf '  OK    %s\n' "$name"
  else
    printf '  FAIL  %s\n' "$name"
    sed 's/^/        /' <<<"$out" | tail -n 5
    failed=1
  fi
}

check "kafka            broker answers" \
  kubectl exec kafka-0 -c kafka -- kafka-broker-api-versions --bootstrap-server localhost:9092
# The Confluent images have no curl, so these send the HTTP request from bash.
http_get='exec 3<>/dev/tcp/localhost/$0 && printf "GET $1 HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3 && head -n 1 <&3 | grep -q " 200 "'
check "schema-registry  REST API answers" \
  kubectl exec deploy/schema-registry -c schema-registry -- bash -c "$http_get" 8081 /subjects
check "ksqldb           REST API answers" \
  kubectl exec deploy/ksqldb -c ksqldb -- bash -c "$http_get" 8088 /info
check "redis            PING" \
  bash -c 'kubectl exec deploy/redis -c redis -- redis-cli ping | grep -q PONG'
check "hdfs             datanode registered with namenode" \
  bash -c "kubectl exec hdfs-namenode-0 -c namenode -- hdfs dfsadmin -report | grep -q 'Live datanodes ([1-9]'"
check "cassandra        CQL answers" \
  kubectl exec cassandra-0 -c cassandra -- cqlsh -e 'describe keyspaces'

echo
if [[ $failed -eq 0 ]]; then
  echo "All services are up."
else
  echo "Some services failed. Look closer with: kubectl get pods, kubectl describe pod <name>, kubectl logs <name>"
fi
exit $failed
