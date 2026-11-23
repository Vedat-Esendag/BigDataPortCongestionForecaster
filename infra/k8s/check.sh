#!/usr/bin/env bash
# Smoke test: is every service up and answering? Read-only, safe to run any time.
#   infra/k8s/check.sh                    the school cluster   (or: make k8s-check)
#   PF_CLUSTER=local infra/k8s/check.sh   the local kind one   (or: make local-check)
set -uo pipefail
cd "$(dirname "$0")/../.."

case "${PF_CLUSTER:-school}" in
  school) source infra/k8s/cluster/env.sh >/dev/null || exit 1 ;;
  local) source infra/k8s/kind/env.sh >/dev/null || exit 1 ;;
  *) echo "PF_CLUSTER must be school or local" >&2; exit 2 ;;
esac

echo "Waiting up to 5 minutes for every pod to be ready..."
# Pods of finished Jobs (cassandra-init, hdfs-init) never become Ready, so they are left out of the wait.
# Whether such a Job did its work is checked below (the keyspace check).
kubectl wait pod -l app.kubernetes.io/part-of=port-forecaster \
  --field-selector=status.phase!=Succeeded,status.phase!=Failed \
  --for=condition=Ready --timeout=300s >/dev/null ||
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
# Both settings come from redis.conf. Without them Redis forgets everything on restart, or the
# dashboard gets no live updates.
check "redis            AOF on, keyspace events on" \
  bash -c 'kubectl exec deploy/redis -c redis -- redis-cli config get appendonly | grep -qx yes &&
           kubectl exec deploy/redis -c redis -- redis-cli config get notify-keyspace-events | grep -q K'
check "hdfs             datanode registered with namenode" \
  bash -c "kubectl exec hdfs-namenode-0 -c namenode -- hdfs dfsadmin -report | grep -q 'Live datanodes ([1-9]'"
check "hdfs             lake folders exist (hdfs-init job)" \
  kubectl exec hdfs-namenode-0 -c namenode -- bash -c \
  'for d in /lake/raw /lake/quarantine /models/eta /checkpoints; do hdfs dfs -test -d $d || exit 1; done'
check "cassandra        CQL answers" \
  kubectl exec cassandra-0 -c cassandra -- cqlsh -e 'describe keyspaces'
# Created by the cassandra-init Job. If this fails: kubectl logs job/cassandra-init
# (the Job removes itself 5 minutes after it finishes; deploying again reruns it safely).
check "cassandra        keyspace portcongestion exists" \
  kubectl exec cassandra-0 -c cassandra -- cqlsh -e 'describe keyspace portcongestion'

# The wiring between the services: topics, ksqlDB queries, connectors. Checked only where Kafka
# Connect runs (the local stack today; the school cluster once our images have a registry),
# because that is where infra/k8s/local-up.sh has set them up.
if kubectl get deploy kafka-connect >/dev/null 2>&1; then
  check "kafka            project topics exist" \
    bash -c 'kubectl exec kafka-0 -c kafka -- kafka-topics --bootstrap-server localhost:9092 --list |
             grep -qx port.outlook'
  # ksqlDB has no curl either: POST "SHOW QUERIES" from bash, look for the alert query.
  ksql_queries='body="{\"ksql\":\"SHOW QUERIES;\"}"; exec 3<>/dev/tcp/localhost/8088 &&
    printf "POST /ksql HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/vnd.ksql.v1+json\r\nContent-Length: ${#body}\r\n\r\n%s" "$body" >&3 &&
    cat <&3 | grep -q CSAS_ALERTS_OUT'
  check "ksqldb           alert query running" \
    kubectl exec deploy/ksqldb -c ksqldb -- bash -c "$ksql_queries"
  # The Connect image has no curl, but it has python3: the checks run it inside the pod.
  check "kafka-connect    REST API answers" \
    kubectl exec deploy/kafka-connect -c kafka-connect -- python3 -c \
    'import urllib.request; urllib.request.urlopen("http://localhost:8083/connectors", timeout=10)'
  # Every deployed connector and each of its tasks must be RUNNING. Prints the ones that aren't.
  check "kafka-connect    every connector RUNNING" \
    kubectl exec deploy/kafka-connect -c kafka-connect -- python3 -c '
import json, sys, urllib.request
url = "http://localhost:8083/connectors?expand=status"
connectors = json.load(urllib.request.urlopen(url, timeout=10))
bad = []
for name, info in sorted(connectors.items()):
    status = info["status"]
    states = [status["connector"]["state"]] + [task["state"] for task in status["tasks"]]
    if not status["tasks"] or any(state != "RUNNING" for state in states):
        bad.append(name + ": " + ", ".join(states))
print("\n".join(bad) if bad else "%d connectors running" % len(connectors))
sys.exit(1 if bad or not connectors else 0)'
fi

echo
if [[ $failed -eq 0 ]]; then
  echo "All services are up."
else
  echo "Some services failed. Look closer with: kubectl get pods, kubectl describe pod <name>, kubectl logs <name>"
fi
exit $failed
