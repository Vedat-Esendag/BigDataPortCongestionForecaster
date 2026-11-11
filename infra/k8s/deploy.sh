#!/usr/bin/env bash
# Deploy our stack to the course cluster, and nothing else.
#   infra/k8s/deploy.sh diff     show what would change
#   infra/k8s/deploy.sh apply    apply it
#
# The team token can also change other groups' namespaces and cluster-wide objects, so before
# anything reaches the cluster this checks that every object is namespaced and in bd-gr-05.
set -euo pipefail
cd "$(dirname "$0")/../.."

NAMESPACE=bd-gr-05
action="${1:-diff}"

source infra/k8s/cluster/env.sh >/dev/null

rendered="$(kubectl kustomize infra/k8s)"

# kind and namespace of every object, one per line
objects="$(kubectl create --dry-run=client -o jsonpath='{.kind}{"\t"}{.metadata.namespace}{"\n"}' -f - <<<"$rendered")"
bad="$(awk -F'\t' -v ns="$NAMESPACE" '$2 != ns' <<<"$objects")"
if [[ -n "$bad" ]]; then
  echo "Refusing to deploy: these objects are cluster-wide or outside $NAMESPACE:" >&2
  echo "$bad" >&2
  exit 1
fi

case "$action" in
  diff)
    kubectl diff -n "$NAMESPACE" -f - <<<"$rendered" || [[ $? -eq 1 ]] # 1 means "there are differences"
    ;;
  apply)
    kubectl apply -n "$NAMESPACE" -f - <<<"$rendered"
    ;;
  *)
    echo "usage: $0 diff|apply" >&2
    exit 2
    ;;
esac
