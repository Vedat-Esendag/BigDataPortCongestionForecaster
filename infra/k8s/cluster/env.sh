#!/usr/bin/env bash
# Point kubectl and helm at the course cluster for this shell only, and refuse to continue if the
# active namespace is not ours. Use with `source`, not `bash`:
#   source infra/k8s/cluster/env.sh

_pc_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_pc_kubeconfig="$_pc_dir/kubeconfig.yaml"
_pc_namespace=bd-gr-05

if [[ ! -f "$_pc_kubeconfig" ]]; then
  echo "Missing $_pc_kubeconfig. Ask the team for it; it is not in git." >&2
  return 1 2>/dev/null || exit 1
fi

export KUBECONFIG="$_pc_kubeconfig"

_pc_active="$(kubectl config view --minify -o jsonpath='{..namespace}')"
if [[ "$_pc_active" != "$_pc_namespace" ]]; then
  echo "kubeconfig namespace is '$_pc_active', expected '$_pc_namespace'." >&2
  return 1 2>/dev/null || exit 1
fi

echo "KUBECONFIG set. Context $(kubectl config current-context), namespace $_pc_namespace."
unset _pc_dir _pc_kubeconfig _pc_namespace _pc_active
