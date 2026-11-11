# Kubernetes (course cluster)

The base services on the university's shared MicroK8s cluster, in our namespace `bd-gr-05`. How
to get access, and the rules for working on a shared cluster, are in
[cluster/README.md](cluster/README.md).

This is a starting point only: each service starts with the minimum it needs to run, and nothing
is set up for the project yet.

## Deploy

```bash
make k8s-diff     # what would change on the cluster
make k8s-apply    # apply it
```

Both go through [deploy.sh](deploy.sh), which refuses to run if any object is cluster-wide or
outside `bd-gr-05`. New manifests go in [kustomization.yaml](kustomization.yaml).

Check on it:

```bash
source infra/k8s/cluster/env.sh
kubectl get pods                         # all Running
```

## What runs

| File | What |
|---|---|
| [kafka.yaml](kafka.yaml) | Kafka, one broker (KRaft) |
| [schema-registry.yaml](schema-registry.yaml) | Schema Registry |
| [redis.yaml](redis.yaml) | Redis, default settings |
| [hdfs.yaml](hdfs.yaml) | HDFS, one namenode and one datanode |
| [cassandra.yaml](cassandra.yaml) | Cassandra, one node |

Not included on purpose: no topics, schemas, HDFS folders, keyspaces or tables, and no persistent
storage (every service loses its data when its pod restarts). Image tags repeat `versions.env`; change both
together.

## Addresses

Anything in the cluster can reach these (there are no network policies). Inside `bd-gr-05` the
short names (`kafka:29092`) work as well.

| Service | Address from any namespace |
|---|---|
| Kafka | `kafka.bd-gr-05.svc.cluster.local:29092` (plaintext, no auth) |
| Schema Registry | `http://schema-registry.bd-gr-05.svc.cluster.local:8081` |
| Redis | `redis.bd-gr-05.svc.cluster.local:6379` |
| HDFS | `hdfs://namenode.bd-gr-05.svc.cluster.local:8020`, web UI on port 9870 |
| Cassandra | `cassandra.bd-gr-05.svc.cluster.local:9042` |

From a laptop, `kubectl port-forward svc/<name> <port>` works for Schema Registry, Redis,
Cassandra and the HDFS web UI, but not for Kafka or HDFS reads and writes: Kafka and the
namenode send clients in-cluster addresses.

## Updating a running setup

No need to stop anything. Change the files, then:

```bash
make k8s-diff     # check that only what you meant changes
make k8s-apply
```

- A new file (add it to `kustomization.yaml`) is created; the rest stays as it is.
- A changed service gets a fresh pod. With no persistent storage, its data starts empty again.
- A file you remove is **not** deleted from the cluster. Delete it there yourself, before you
  remove it from `kustomization.yaml`: `kubectl delete -f infra/k8s/<file>.yaml -n bd-gr-05`.

## Stop

```bash
kubectl delete -k infra/k8s -n bd-gr-05
```
