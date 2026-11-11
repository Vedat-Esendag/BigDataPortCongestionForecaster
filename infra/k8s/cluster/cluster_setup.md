# Connecting to the school Kubernetes cluster

This is how you get from a fresh laptop to running `kubectl` against our namespace on the
school cluster. It takes about 15 minutes the first time. Once you're connected, the steps
for deploying our services are in [../README.md](../README.md).

We do all our Kubernetes work on this one cluster: development, testing and the final demo.
There's no local cluster to set up.

## What you need

**1. The school network.** The cluster is only reachable from inside the university network.
Either be on the university Wi-Fi, or connect through the school VPN, **Cisco AnyConnect**
(the university IT pages explain how to install it and log in). If you skip this, every
`kubectl` command just hangs and ends with an `i/o timeout`.

**2. kubectl.** The cluster runs Kubernetes 1.35, so get kubectl 1.34, 1.35 or 1.36. A newer one
mostly works, but prints a version warning and can behave oddly.

- macOS: `brew install kubectl`
- Ubuntu / WSL: `sudo snap install kubectl --classic --channel=1.35/stable`
- Check it: `kubectl version --client`

**3. The team kubeconfig.** This is a small file with our login token in it. Ask the person
who manages our cluster access, and you'll get it sent directly. Don't ask for it in the group
chat, and don't pass it on: anyone holding it can change things for every group on the
cluster, not just ours.

## Setting it up

Put the kubeconfig in this folder, with exactly this name:

```
infra/k8s/cluster/kubeconfig.yaml
```

Git ignores that file, so you can't commit it by accident. Still, keep it out of screenshots,
pastes and other folders. On macOS or Linux, make it readable only by you:

```bash
chmod 600 infra/k8s/cluster/kubeconfig.yaml
```

Then, from the repo root, in every terminal where you want to use the cluster:

```bash
source infra/k8s/cluster/env.sh
```

It should print `KUBECONFIG set. Context bd-gr-05-context, namespace bd-gr-05.` This only
affects that one terminal. Your own `~/.kube/config` (if you have one) stays as it is, so
open a new terminal and you're back to normal.

## Checking that it works

```bash
kubectl get pods
```

If you get a list of pods, or `No resources found in bd-gr-05 namespace`, you're connected.
The next section explains what the list should look like.

If it doesn't work:

| What you see | What it means |
|---|---|
| Hangs, then `i/o timeout` | You're not on the university network. Connect AnyConnect or the uni Wi-Fi |
| `Missing .../kubeconfig.yaml` | The file isn't in `infra/k8s/cluster/`, or has a different name |
| `Unauthorized` | The token is wrong or was replaced. Ask for the current kubeconfig |
| `kubectl: command not found` | kubectl isn't installed, or not on your PATH |
| Version skew warning | Your kubectl is too new or too old; see "What you need" above |

## Looking at what's running

Everything in this section only reads, so it's safe to run any time, even while someone else
is deploying. Run `source infra/k8s/cluster/env.sh` first in each new terminal.

### The pods

```bash
kubectl get pods
```

When everything is up, you should see seven pods, all `1/1` and `Running`:

```
NAME                               READY   STATUS    RESTARTS   AGE
cassandra-0                        1/1     Running   0          5m
hdfs-datanode-0                    1/1     Running   0          5m
hdfs-namenode-0                    1/1     Running   0          5m
kafka-0                            1/1     Running   0          5m
ksqldb-6d9f8c7b5-x2k4p             1/1     Running   0          5m
redis-5b7c9d8f6-q8w2n              1/1     Running   0          5m
schema-registry-7c8d9f6b4-m3j7k    1/1     Running   0          5m
```

The random endings on `ksqldb`, `redis` and `schema-registry` change every time those pods
restart, so copy the names from your own output. How to read the columns:

- **READY `0/1`** means the pod is running but not answering yet. Normal for the first minute or
  two after a deploy; Cassandra is usually the slowest.
- **STATUS `Init:0/1`** means it's waiting for something else first: Schema Registry and ksqlDB
  wait for Kafka, the datanode waits for the namenode.
- **STATUS `CrashLoopBackOff`**, or a **RESTARTS** count that keeps climbing, means it keeps
  failing. Look at its logs (below) and tell the group.

To watch it change live, add `-w` and press Ctrl+C to stop: `kubectl get pods -w`.

### Is everything actually working?

A pod can be `Running` and still not work. This checks each service for real (from the repo
root, read-only):

```bash
make k8s-check
```

It ends with `All services are up.`, or tells you which service failed.

### Why is a pod unhappy?

```bash
kubectl describe pod kafka-0           # the Events at the bottom say why it isn't starting
kubectl logs kafka-0                   # what the program printed
kubectl logs kafka-0 --previous        # what it printed before its last crash
kubectl logs kafka-0 -f                # follow the output live (Ctrl+C to stop)
```

For a pod with a random ending, use the workload's name instead, so you don't need to copy it:
`kubectl logs deploy/schema-registry`.

### Looking inside the services

Each service is empty for now, but these show that it answers and what it holds:

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list
kubectl exec cassandra-0 -- cqlsh -e 'describe keyspaces'
kubectl exec deploy/redis -- redis-cli info keyspace
kubectl exec hdfs-namenode-0 -- hdfs dfs -ls /
```

The services only have addresses inside the cluster. To open one from your laptop, forward a
port in one terminal (it keeps running until Ctrl+C), then use `localhost` in another:

```bash
kubectl port-forward svc/hdfs-namenode 9870      # HDFS web UI: http://localhost:9870
kubectl port-forward svc/schema-registry 8081    # http://localhost:8081/subjects
kubectl port-forward svc/ksqldb 8088             # http://localhost:8088/info
```

This doesn't work for Kafka itself: the broker sends clients its in-cluster name, which your
laptop can't resolve. Use the `kubectl exec kafka-0 ...` commands instead.

### Everything else in the namespace

```bash
kubectl get svc                  # our services and their ports
kubectl get all                  # pods, services, deployments and statefulsets together
```

You'll also see a few things we didn't create: the `bd-gr-05-sa` service account, its token
secret, `bd-gr-05-role` and its binding, and `kube-root-ca.crt`. The school set those up, and our
login depends on them. **Never delete them.**

## Rules for the shared cluster

Every group, plus a lot of students, runs on the same machine. Our token has more rights than it
should: it can also change other groups' namespaces and cluster-wide settings. So, please:

- **Only work in `bd-gr-05`.** Never use `-A` or `--all-namespaces` with anything that
  creates, changes or deletes. `env.sh` already points you at our namespace, so you don't need
  `-n` at all.
- **Don't install anything cluster-wide**: operators, CRDs, ClusterRoles, or Helm charts that
  create them. These are shared by everyone, and only the course staff should add them.
- **Don't install a second Prometheus.** There is already one for the whole cluster. Whether we
  may use it is still an open question.
- **Change the cluster through the files in the repo, not by hand.** Edit the manifests in
  `infra/k8s/`, then deploy with `make k8s-diff` and `make k8s-up` (see [../README.md](../README.md)).
  That way everyone can see what's running, and we can rebuild it if something breaks.
- **Tell the group before you deploy or delete something.** Development and the final demo
  share one namespace, so one person's experiment can break everyone else's work.

## What the cluster looks like

Checked on 10 November 2026; it may have changed since.

| | |
|---|---|
| Distribution | MicroK8s, Kubernetes 1.35, a single node shared by every group and student |
| Our namespace | `bd-gr-05`. Other groups have `bd-gr-01` to `bd-gr-11` |
| Storage | `hostpath-custom` (the default) and `microk8s-hostpath`, both on the node's own disk |
| Already installed (not ours) | cert-manager, and a Prometheus and Grafana stack in the `observability` namespace |
| Not available | an Ingress controller (use `port-forward`), a metrics server (`kubectl top` doesn't work), an image registry |
| Load | about 30% of the CPU and 9% of the memory reserved, across roughly 500 pods |
