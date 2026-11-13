#!/usr/bin/env bash
# Checks that HDFS keeps its data when its pods restart: writes a test file, deletes both HDFS
# pods, waits for them to come back and reads the file again. Removes the test file at the end.
#   infra/k8s/test-hdfs-restart.sh      (or: make k8s-test-hdfs)
# Restarts the namenode and datanode, so HDFS is unavailable for a minute or two. Tell the group
# first if someone is using it.
set -euo pipefail
cd "$(dirname "$0")/../.."

source infra/k8s/cluster/env.sh >/dev/null || exit 1

file=/tmp/restart-test-$(date +%s).txt
text="written before restart at $(date -u +%FT%TZ)"
nn() { kubectl exec hdfs-namenode-0 -c namenode -- "$@"; }

wait_ready() {
  kubectl rollout status statefulset/hdfs-namenode --timeout=300s >/dev/null
  kubectl rollout status statefulset/hdfs-datanode --timeout=300s >/dev/null
  echo "  waiting for the namenode to leave safe mode and see the datanode..."
  nn hdfs dfsadmin -safemode wait >/dev/null
  for _ in $(seq 60); do
    nn hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes ([1-9]' && return 0
    sleep 3
  done
  echo "FAIL: no live datanode after 3 minutes"
  exit 1
}

read_back() { # what to call this step
  local got
  got="$(nn hdfs dfs -cat "$file")" || { echo "FAIL: $file is gone after $1"; exit 1; }
  [ "$got" = "$text" ] || { echo "FAIL: $file changed after $1: $got"; exit 1; }
  echo "  OK: file still there after $1"
}

echo "1. Write a test file"
wait_ready
nn bash -c "echo '$text' | hdfs dfs -put -f - '$file'"
echo "  wrote $file"

echo "2. Restart the namenode (metadata must survive)"
kubectl delete pod hdfs-namenode-0 --wait=true >/dev/null
sleep 5
wait_ready
read_back "a namenode restart"

echo "3. Restart the datanode (blocks must survive)"
kubectl delete pod hdfs-datanode-0 --wait=true >/dev/null
sleep 5
wait_ready
read_back "a datanode restart"

echo "4. Restart both at once"
kubectl delete pod hdfs-namenode-0 hdfs-datanode-0 --wait=true >/dev/null
sleep 5
wait_ready
read_back "restarting both"

nn hdfs dfs -rm -skipTrash "$file" >/dev/null
echo
echo "HDFS keeps its data across pod restarts."
