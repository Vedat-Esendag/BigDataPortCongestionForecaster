#!/usr/bin/env bash
# Point kubectl at the LOCAL kind cluster "pf", namespace bd-gr-05, for this shell only. The
# local counterpart of ../cluster/env.sh. Use with `source`, not `bash`:
#   source infra/k8s/kind/env.sh
#
# It writes a kubeconfig holding only the kind cluster to infra/k8s/kind/kubeconfig.yaml
# (gitignored) and exports KUBECONFIG. Every kubectl in this shell then talks to kind, whatever
# your default context is, so nothing here can reach the school cluster by accident.

_pf_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_pf_kubeconfig="$_pf_dir/kubeconfig.yaml"
_pf_cluster="${PF_KIND_CLUSTER:-pf}"

if ! kind get clusters 2>/dev/null | grep -qx "$_pf_cluster"; then
  echo "No kind cluster named '$_pf_cluster'. Start one with: make local-up" >&2
  unset _pf_dir _pf_kubeconfig _pf_cluster
  return 1 2>/dev/null || exit 1
fi

# Written to a temporary file first and then moved into place in one step, namespace already set.
# Another terminal (or make local-up) using the same file never sees it half written.
_pf_tmp="$(mktemp "$_pf_dir/.kubeconfig.XXXXXX")"
kind get kubeconfig --name "$_pf_cluster" >"$_pf_tmp"
KUBECONFIG="$_pf_tmp" kubectl config set-context --current --namespace bd-gr-05 >/dev/null
chmod 600 "$_pf_tmp"
mv -f "$_pf_tmp" "$_pf_kubeconfig"
export KUBECONFIG="$_pf_kubeconfig"
unset _pf_tmp

echo "KUBECONFIG set. Context $(kubectl config current-context), namespace bd-gr-05 (local kind)."
unset _pf_dir _pf_kubeconfig _pf_cluster
