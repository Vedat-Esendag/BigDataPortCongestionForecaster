# Kubernetes

The project's services on Kubernetes, in namespace `bd-gr-05`, in two places:

- **Your laptop:** `make local-up` starts the whole stack in a local kind cluster, wires it
  together and checks it. Everything about that is in
  [overlays/local/README.md](overlays/local/README.md).
- **The school cluster:** the rest of this page. To get access first, follow
  [cluster/cluster_setup.md](cluster/cluster_setup.md).

Kafka, HDFS, Cassandra and Redis keep their data on persistent volumes. The HDFS folders and the
Cassandra keyspace and tables are created by init Jobs on every deploy (see
[docs/storage.md](../../docs/storage.md)). Kafka Connect and the AIS producer (in `apps/`) run
locally only so far: their images are on ghcr.io and mapped in `overlays/school` (see Images),
but `apps/` is not part of the school overlay yet.

## Start, check, update

From the repo root, on the university network or VPN:

```bash
make k8s-diff     # what would change on the cluster
make k8s-up       # deploy, then wait until every service answers
```

`make k8s-up` is `make k8s-apply` followed by `make k8s-check`. You can run the check alone at
any time; it only reads. It ends with `All services are up.` or shows which service failed.

To update while things are running, don't stop anything: change the files, then
`make k8s-diff` and `make k8s-apply` again.

- New objects are created, unchanged ones are left alone.
- A changed service gets a fresh pod. The stores keep their data across that.
- The Cassandra schema Job (`cassandra-init`) shows up as new in every diff. That is expected:
  it removes itself five minutes after it finishes, and running it again changes nothing.
- A file you remove from the setup is **not** removed from the cluster. Delete its objects first
  (`kubectl delete -k` on its folder, or `kubectl delete <kind> <name>`), then take it out.

Everything goes through [deploy.sh](deploy.sh), which refuses to run if anything is cluster-wide
or outside `bd-gr-05`.

## Layout

```
infra/k8s/
  base/                 every component, no namespace
    <component>/        kustomization.yaml, service.yaml, statefulset.yaml or deployment.yaml
  apps/                 our own images: kafka-connect, ais-producer
  overlays/school/      namespace bd-gr-05 and the school-specific settings
  overlays/local/       the whole stack on a local kind cluster (see its README.md)
  cluster/              access to the school cluster (kubeconfig, env.sh, setup guide)
  kind/                 the local kind cluster (cluster.yaml, env.sh)
  spark/                Spark jobs on the school cluster (see its README.md)
  deploy.sh             school: diff / apply, with the namespace guard
  local-up.sh           local: the one-command start (make local-up / local-down)
  check.sh              smoke test, school or local (PF_CLUSTER=local)
```

To add a component: create `base/<name>/` like the others, add it to `base/kustomization.yaml`,
then run `kubectl kustomize infra/k8s/overlays/school` to see the result.

## What runs

| Component | Kind | Address in the namespace |
|---|---|---|
| `kafka` | StatefulSet, 1 broker (KRaft) | `kafka:9092` |
| `schema-registry` | Deployment | `http://schema-registry:8081` |
| `ksqldb` | Deployment | `http://ksqldb:8088` |
| `redis` | Deployment, volume `data-redis`, AOF and keyspace events on | `redis:6379` |
| `hdfs-namenode`, `hdfs-datanode` | StatefulSets, headless Services | `hdfs://hdfs-namenode:8020`, web UI on 9870 |
| `cassandra` | StatefulSet, 1 node, volume `data-cassandra-0` | `cassandra:9042` |
| `cassandra-init` | Job, runs `cassandra/schema.cql` | removes itself 5 minutes after finishing |
| `pf-config` | ConfigMap, empty | shared settings, filled in later |
| `spark` | ServiceAccount, Role, RoleBinding | lets Spark drivers start executor pods |

Schema Registry and ksqlDB wait for Kafka before starting, and the datanode waits for the
namenode, so a cold start doesn't crash-loop. Every container runs as a non-root user, has CPU
and memory limits, and has health checks.

Only pods in `bd-gr-05` can use these: Kafka hands out the short name `kafka:9092`, which other
namespaces can't resolve. From a laptop, `kubectl port-forward svc/<name> <port>` works for the
REST APIs, Redis, Cassandra and the HDFS web UI, but not for Kafka or HDFS file reads and writes.

Image tags repeat `versions.env`; change both together.

## Images

Our own images go to `ghcr.io/vedat-esendag` as public packages, so the school node pulls them
without a pull secret. All five use the same tag, the short git commit:

| Image | Built from (context) | Used by |
|---|---|---|
| `spark-jobs` | `spark/Dockerfile` (repo root) | `spark/school` (recorders, one-off Jobs) |
| `pf/ais-producer` | `services/ais-producer/Dockerfile` (repo root) | `apps/ais-producer` |
| `pf/kafka-connect` | `connect/Dockerfile` (`connect/`) | `apps/connect` |
| `pf/api` | `services/api/Dockerfile` (repo root) | no manifest yet |
| `pf/dashboard` | `services/dashboard/Dockerfile` (`services/dashboard/`) | no manifest yet |

```bash
make images-build   # linux/amd64 into local Docker, no push
make images-push    # linux/amd64, pushed as ghcr.io/vedat-esendag/<image>:<sha>
```

The build args come from `versions.env`, the same as in docker compose. `images-push` refuses
a tree with uncommitted or untracked files, so a tag always names a pushed commit. `IMAGES=` picks
a subset, for example `make images-push IMAGES=pf/ais-producer`; `REGISTRY=` changes the
registry. You need `docker login ghcr.io` first, and a new package must be made public once on
GitHub.

## Secrets

Keys (aisstream.io, DMI, the alert webhook) go in `.env`, never in a manifest. Then:

```bash
make k8s-secrets
```

This creates `aisstream-secret`, `dmi-secret` and `alert-webhook-secret` in our namespace, and
skips any key that's empty in `.env`. Run it again after changing a key.

## Stop

```bash
kubectl delete -k infra/k8s/overlays/school
```

This also deletes the Redis volume claim, so Redis starts empty next time. Cassandra's claim
(`data-cassandra-0`) is kept: Kubernetes never deletes claims that a StatefulSet created. To
restart one service without losing its data, delete only its pod
(`kubectl delete pod cassandra-0`, `kubectl rollout restart deploy/redis`).

## Moving from the first version (once)

The first version used other names and labels, and Kubernetes can't change those on running
objects. Delete the old objects once, then start the new ones:

```bash
source infra/k8s/cluster/env.sh
kubectl delete deploy,svc,configmap -l app.kubernetes.io/part-of=port-congestion
make k8s-up
```

The `-l` only matches objects from the first version, all of them in `bd-gr-05`.
