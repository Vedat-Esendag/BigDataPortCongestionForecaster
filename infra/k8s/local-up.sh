#!/usr/bin/env bash
# The whole stack on a local kind cluster, in one command. From the repo root:
#
#   make local-up       (or infra/k8s/local-up.sh)     create or update, set up, check
#   make local-check    (PF_CLUSTER=local infra/k8s/check.sh)
#   make local-down     (infra/k8s/local-up.sh down)   delete the cluster and all its data
#
# What `up` does, in order. Every step is safe to repeat, so running it again on a running
# cluster updates it instead of starting over.
#   1. Check the tools: docker, kind, kubectl, python3.
#   2. Create the kind cluster "pf" if it doesn't exist (infra/k8s/kind/cluster.yaml).
#   3. Build our images (Kafka Connect with its plugins, the AIS producer), tag them with the git
#      commit, and load them into kind. No registry needed.
#   4. Create namespace bd-gr-05 and the Secrets from .env (if there is one).
#   5. Apply infra/k8s/overlays/local (everything except the Spark test Jobs).
#   6. Wait until every service is ready and the HDFS and Cassandra init Jobs are done.
#   7. Wire the services together: Kafka topics, Avro schemas, ksqlDB queries, connectors.
#   8. Run the smoke test (check.sh).
#
# It only ever talks to the kind cluster: kubectl runs with a kubeconfig that holds nothing else
# (infra/k8s/kind/env.sh), so it can't touch the school cluster even if that is your default.
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=pf
NAMESPACE=bd-gr-05
OVERLAY=infra/k8s/overlays/local
# Local ports for talking to the REST APIs during setup. Not 8081/8083/8088, so the compose stack
# can keep running alongside.
PORT_REGISTRY=18081
PORT_KSQLDB=18088
PORT_CONNECT=18083

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
die() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

# ----------------------------------------------------------------------------- down

if [[ "${1:-up}" == "down" ]]; then
  step "Deleting kind cluster $CLUSTER (all its data goes with it)"
  kind delete cluster --name "$CLUSTER" --kubeconfig infra/k8s/kind/kubeconfig.yaml
  rm -f infra/k8s/kind/kubeconfig.yaml
  exit 0
fi
[[ "${1:-up}" == "up" ]] || die "usage: $0 [up|down]"

# ----------------------------------------------------------------------------- 1. tools

step "1/8 Checking tools"
for tool in docker kind kubectl python3 git; do
  command -v "$tool" >/dev/null || die "$tool is not installed. See infra/k8s/overlays/local/README.md"
done
docker info >/dev/null 2>&1 || die "Docker is not running"
if ! command -v uv >/dev/null && ! python3 -c 'import yaml' 2>/dev/null; then
  die "the connector script needs uv, or a python3 with PyYAML (pip install pyyaml)"
fi
mem_gib=$(( $(docker info --format '{{.MemTotal}}') / 1024 / 1024 / 1024 ))
note "Docker has ${mem_gib} GiB of memory"
(( mem_gib >= 7 )) || note "WARNING: the stack needs about 8 GiB. Give Docker more memory if pods stay Pending."

# ----------------------------------------------------------------------------- 2. cluster

step "2/8 kind cluster $CLUSTER"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  note "exists, reusing it (make local-down deletes it)"
else
  # --kubeconfig: kind would otherwise add the cluster to ~/.kube/config and make it your default
  # context. This keeps your default as it was; env.sh points this script (and you) at kind.
  kind create cluster --name "$CLUSTER" --config infra/k8s/kind/cluster.yaml --wait 120s \
    --kubeconfig infra/k8s/kind/kubeconfig.yaml
fi
# From here on every kubectl talks to kind only, in namespace bd-gr-05. The script works on its
# own copy of the kubeconfig, so another terminal running env.sh meanwhile can't affect it.
source infra/k8s/kind/env.sh >/dev/null
LAYER="$(mktemp -d)"
PF_PIDS=()
cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
  rm -rf "$LAYER"
}
trap cleanup EXIT
cp "$KUBECONFIG" "$LAYER/kubeconfig"
export KUBECONFIG="$LAYER/kubeconfig"

# ----------------------------------------------------------------------------- 3. images

step "3/8 Building our images and loading them into kind"
# Tagged with the commit, plus "-dirty" when there are uncommitted changes, so a tag never claims
# to be a commit it isn't. Never "latest".
TAG="$(git rev-parse --short HEAD)$([[ -n "$(git status --porcelain)" ]] && echo -dirty || true)"
note "tag $TAG"
set -a
source versions.env
set +a
docker build -q -t "pf/kafka-connect:$TAG" \
  --build-arg CP_VERSION --build-arg FILEPULSE_VERSION --build-arg CAMEL_HTTP_SOURCE_VERSION \
  --build-arg HDFS_SINK_VERSION --build-arg CASSANDRA_SINK_VERSION --build-arg REDIS_SINK_VERSION \
  --build-arg LENSES_HTTP_SINK_VERSION \
  connect >/dev/null
note "built pf/kafka-connect:$TAG"
docker build -q -t "pf/ais-producer:$TAG" \
  --build-arg PYTHON_VERSION --build-arg UV_VERSION \
  -f services/ais-producer/Dockerfile . >/dev/null
note "built pf/ais-producer:$TAG"
kind load docker-image --name "$CLUSTER" "pf/kafka-connect:$TAG" "pf/ais-producer:$TAG" >/dev/null
note "loaded both into kind"

# The overlay plus the image tags, in a throwaway layer: the repo's files stay untouched.
cat >"$LAYER/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - $(python3 -c 'import os, sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))' "$PWD/$OVERLAY" "$LAYER")
images:
  - name: pf/kafka-connect
    newTag: "$TAG"
  - name: pf/ais-producer
    newTag: "$TAG"
EOF
kubectl kustomize "$LAYER" >"$LAYER/rendered.yaml"

# ----------------------------------------------------------------------------- 4. namespace, secrets

step "4/8 Namespace and Secrets"
kubectl apply -l 'app.kubernetes.io/component=namespace' -f "$LAYER/rendered.yaml" >/dev/null
note "namespace $NAMESPACE ready"
HAVE_AIS_KEY=0
if [[ -f .env ]]; then
  PF_CLUSTER=local scripts/create_k8s_secrets.sh | sed 's/^/   /'
  grep -qE '^AISSTREAM_API_KEY=.+' .env && HAVE_AIS_KEY=1
else
  note "no .env: no Secrets. The AIS producer stays off, and the DMI and webhook connectors"
  note "are skipped. Copy .env.example to .env and fill in the keys to turn them on."
fi

# ----------------------------------------------------------------------------- 5. apply

step "5/8 Applying $OVERLAY"
# A Job can't be changed once created. The init Jobs are safe to repeat, so remove finished
# ones first; the apply below then creates them fresh.
kubectl delete job -l 'app.kubernetes.io/name in (hdfs-init,cassandra-init)' \
  --field-selector status.successful=1 --ignore-not-found >/dev/null
# Everything except the Spark test Jobs (label spark-job), which are run by hand.
kubectl apply -l '!spark-job' -f "$LAYER/rendered.yaml" | sed 's/^/   /'
if (( ! HAVE_AIS_KEY )); then
  kubectl scale deploy/ais-producer --replicas=0 >/dev/null
  note "ais-producer scaled to 0 (no AISSTREAM_API_KEY in .env)"
fi

# ----------------------------------------------------------------------------- 6. wait

step "6/8 Waiting for the services (the first start downloads about 5 GB of images)"
for workload in statefulset/kafka deploy/schema-registry deploy/ksqldb deploy/kafka-connect \
  deploy/redis statefulset/hdfs-namenode statefulset/hdfs-datanode statefulset/cassandra; do
  kubectl rollout status "$workload" --timeout=900s >/dev/null || {
    kubectl get pods
    die "$workload did not become ready. Look at: kubectl describe ${workload/statefulset/sts}"
  }
  note "ready: $workload"
done
for job in hdfs-init cassandra-init; do
  kubectl wait --for=condition=complete "job/$job" --timeout=600s >/dev/null ||
    die "job $job did not finish. Look at: kubectl logs job/$job"
  note "done:  job/$job"
done

# ----------------------------------------------------------------------------- 7. wiring

step "7/8 Wiring: topics, schemas, ksqlDB queries, connectors"
# kubectl port-forward runs in the background for the REST calls and is stopped at exit.
forward() { # service, local port, remote port
  kubectl port-forward "svc/$1" "$2:$3" >/dev/null 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 30); do
    (echo >"/dev/tcp/127.0.0.1/$2") 2>/dev/null && return 0
    sleep 1
  done
  die "port-forward to $1 did not come up"
}
forward schema-registry "$PORT_REGISTRY" 8081
forward ksqldb "$PORT_KSQLDB" 8088
forward kafka-connect "$PORT_CONNECT" 8083

note "Kafka topics:"
KAFKA_EXEC="kubectl exec -i kafka-0 -c kafka --" KAFKA_INTERNAL_BOOTSTRAP=localhost:9092 \
  scripts/create_topics.sh 2>&1 | grep -v -E '^(Created topic|$)' | sed 's/^/     /' || true
note "Avro schemas:"
python3 scripts/register_schemas.py --url "http://127.0.0.1:$PORT_REGISTRY" | sed 's/^/     /'
note "ksqlDB queries:"
python3 scripts/run_ksql.py --url "http://127.0.0.1:$PORT_KSQLDB" | sed 's/^/     /'
note "Kafka Connect connectors:"
connectors_ok=1
CONNECT_URL="http://127.0.0.1:$PORT_CONNECT" scripts/deploy_connectors.sh 2>&1 | sed 's/^/     /' ||
  connectors_ok=0
if (( HAVE_AIS_KEY )); then
  # Restart it now that the schemas exist, so it doesn't sit in a retry loop from before.
  kubectl rollout restart deploy/ais-producer >/dev/null
  note "ais-producer restarted with the schemas in place"
fi

# ----------------------------------------------------------------------------- 8. check

step "8/8 Smoke test"
check_ok=1
PF_CLUSTER=local infra/k8s/check.sh || check_ok=0

echo
if (( check_ok && connectors_ok )); then
  printf '\033[1mThe local stack is up.\033[0m '
else
  printf '\033[1mThe local stack is up, with problems listed above.\033[0m '
fi
cat <<EOF
Use it from this terminal with:

  source infra/k8s/kind/env.sh                       # kubectl now talks to kind, namespace $NAMESPACE
  kubectl get pods
  kubectl port-forward svc/kafka-connect 8083        # http://localhost:8083/connectors
  kubectl port-forward svc/hdfs-namenode 9870        # HDFS web UI: http://localhost:9870

Stop and delete everything: make local-down
EOF
(( check_ok && connectors_ok ))
