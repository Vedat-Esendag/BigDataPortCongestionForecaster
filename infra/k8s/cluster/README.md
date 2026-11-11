# Course cluster access

How the team connects to the shared course Kubernetes cluster, and what we found out about it.
Deploying is described in [../README.md](../README.md).

## Connect

1. Put the team kubeconfig in this folder as `kubeconfig.yaml`. It holds a service-account token,
   so it is git-ignored. Share it with teammates directly, never through the repo or chat logs.
2. In each shell where you work on the cluster:

   ```bash
   source infra/k8s/cluster/env.sh
   kubectl get pods            # empty at first; "No resources found" means the connection works
   ```

`env.sh` only sets `KUBECONFIG` for the current shell, so your own `~/.kube/config` stays untouched.

## What the cluster looks like (checked 2026-11-10)

| | |
|---|---|
| Distribution | MicroK8s, Kubernetes 1.35, a **single node** shared by every group and student |
| Our namespace | `bd-gr-05` (the context's default). Other groups have `bd-gr-01` to `bd-gr-11` |
| Our rights | Wider than they need to be: the service account can create and delete almost anything, **also outside our namespace** (namespaces, CRDs, ClusterRoles) |
| Storage | `hostpath-custom` (default StorageClass) and `microk8s-hostpath`, both node-local |
| Already installed | cert-manager; kube-prometheus-stack in `observability`, which picks up any ServiceMonitor labelled `release: kube-prom-stack` from any namespace |
| Not installed | Strimzi, K8ssandra, an IngressClass, a metrics-server, an in-cluster image registry |
| Load | About 30% of CPU requested and 9% of memory, across roughly 500 pods |

## Team rules

- **Only touch `bd-gr-05`.** Our rights reach other groups' namespaces too, so one wrong
  `-n`, `-A` or cluster-scoped `delete` can break another team's work. Never pass `-A` to a
  write command.
- **Don't install cluster-wide things** (operators, CRDs, ClusterRoles, Helm charts that create
  them) without the course staff agreeing first. CRDs are shared by everyone on the cluster.
- **Don't install a second Prometheus.** Use the shared one through ServiceMonitors.
