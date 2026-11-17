#!/usr/bin/env bash
# HDFS health at a glance: datanodes, free space, block problems and the size of each lake zone.
# Read-only, safe to run any time.
#   infra/k8s/hdfs-report.sh      (or: make k8s-hdfs-report)
# Exits 1 when HDFS is in safe mode, has no live datanode, or has missing or corrupt blocks.
set -uo pipefail
cd "$(dirname "$0")/../.."

source infra/k8s/cluster/env.sh >/dev/null || exit 1

nn() { kubectl exec hdfs-namenode-0 -c namenode -- "$@" 2>/dev/null; }

report="$(nn hdfs dfsadmin -report)" || { echo "FAIL: cannot reach the namenode"; exit 1; }
fsck="$(nn hdfs fsck / 2>/dev/null)"
safemode="$(nn hdfs dfsadmin -safemode get)"

value() { grep -m1 "$1" <<<"$2" | sed 's/^[^:]*:[[:space:]]*//'; }

echo "Namenode:  $safemode"
count() { grep -o "$1 datanodes ([0-9]*)" <<<"$report" | grep -o '[0-9]*' || echo 0; }
# A stopped datanode counts as live until the namenode misses its heartbeats for ~10 minutes.
echo "Datanodes: $(count Live) live, $(count Dead) dead (a datanode is marked dead after ~10 min)"
echo "Space:     used $(value 'DFS Used:' "$report"), free $(value 'DFS Remaining:' "$report") of $(value 'Configured Capacity:' "$report")"
echo "Blocks:    total $(value 'Total blocks' "$fsck"), average copies $(value 'Average block replication' "$fsck")"
echo "           under-replicated $(value 'Under-replicated blocks' "$fsck"), missing $(value 'Missing blocks:' "$fsck"), corrupt $(value 'Corrupt blocks' "$fsck")"
echo
echo "Size per folder (one copy, then all copies):"
nn hdfs dfs -du -s -h '/lake/*' /models/eta /checkpoints | sed 's/^/  /'

failed=0
grep -q 'Safe mode is OFF' <<<"$safemode" || { echo "FAIL: namenode is in safe mode"; failed=1; }
grep -q 'Live datanodes ([1-9]' <<<"$report" || { echo "FAIL: no live datanode"; failed=1; }
grep -q 'Status: HEALTHY' <<<"$fsck" || { echo "FAIL: fsck reports missing or corrupt blocks"; failed=1; }
[[ $failed -eq 0 ]] && echo && echo "HDFS is healthy."
exit $failed
