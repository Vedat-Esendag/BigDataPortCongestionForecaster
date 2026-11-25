#!/usr/bin/env bash
# Create or update the Kubernetes Secrets in our namespace from the values in .env. Values never
# appear in the repo, in manifests or on the command line. Run again after changing .env.
#   scripts/create_k8s_secrets.sh                    the school cluster (or: make k8s-secrets)
#   PF_CLUSTER=local scripts/create_k8s_secrets.sh   the local kind cluster (make local-up runs it)
set -euo pipefail
cd "$(dirname "$0")/.."

NAMESPACE=bd-gr-05
PF_CLUSTER="${PF_CLUSTER:-school}"
case "$PF_CLUSTER" in
  school) source infra/k8s/cluster/env.sh >/dev/null ;;
  local) source infra/k8s/kind/env.sh >/dev/null ;;
  *) echo "PF_CLUSTER must be school or local" >&2; exit 2 ;;
esac

if [[ ! -f .env ]]; then
  echo "No .env file. Copy .env.example to .env and fill in the keys first." >&2
  exit 1
fi
set -a
source .env
set +a

# secret name, key, value
create() {
  local name="$1" key="$2" value="$3"
  if [[ -z "$value" ]]; then
    echo "skip    $name ($key is empty in .env)"
    return
  fi
  kubectl create secret generic "$name" -n "$NAMESPACE" \
    --from-file="$key"=<(printf '%s' "$value") --dry-run=client -o yaml |
    kubectl label --local -f - -o yaml \
      app.kubernetes.io/name="${name%-secret}" app.kubernetes.io/part-of=port-forecaster \
      app.kubernetes.io/component=secret layer=tooling env="$PF_CLUSTER" |
    kubectl apply -n "$NAMESPACE" -f - >/dev/null
  echo "applied $name"
}

create aisstream-secret AISSTREAM_API_KEY "${AISSTREAM_API_KEY:-}"
create alert-webhook-secret ALERT_WEBHOOK_URL "${ALERT_WEBHOOK_URL:-}"
