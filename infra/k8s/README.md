# Kubernetes (school cluster)

The base services of the project on the university's shared Kubernetes cluster, in our
namespace `bd-gr-05`. To get access first, follow [cluster/cluster_setup.md](cluster/cluster_setup.md).

This is a starting point: every service starts and answers, but nothing is set up for the
project yet. There are no topics, schemas, HDFS folders, keyspaces or tables, and no persistent
storage, so every service loses its data when its pod restarts.

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
- A changed service gets a fresh pod, which starts empty again (no persistent storage yet).
- A file you remove from the setup is **not** removed from the cluster. Delete its objects first
  (`kubectl delete -k` on its folder, or `kubectl delete <kind> <name>`), then take it out.

Everything goes through [deploy.sh](deploy.sh), which refuses to run if anything is cluster-wide
or outside `bd-gr-05`.

## Layout

```
infra/k8s/
  base/                 every component, no namespace
    <component>/        kustomization.yaml, service.yaml, statefulset.yaml or deployment.yaml
  overlays/school/      namespace bd-gr-05 and the school-specific settings
  cluster/              access to the cluster (kubeconfig, env.sh, setup guide)
  deploy.sh             diff / apply, with the namespace guard
  check.sh              smoke test
```

To add a component: create `base/<name>/` like the others, add it to `base/kustomization.yaml`,
then run `kubectl kustomize infra/k8s/overlays/school` to see the result.

## What runs

| Component | Kind | Address in the namespace |
|---|---|---|
| `kafka` | StatefulSet, 1 broker (KRaft) | `kafka:9092` |
| `schema-registry` | Deployment | `http://schema-registry:8081` |
| `ksqldb` | Deployment | `http://ksqldb:8088` |
| `redis` | Deployment | `redis:6379` |
| `hdfs-namenode`, `hdfs-datanode` | StatefulSets, headless Services | `hdfs://hdfs-namenode:8020`, web UI on 9870 |
| `cassandra` | StatefulSet, 1 node | `cassandra:9042` |
| `pf-config` | ConfigMap, empty | shared settings, filled in later |
| `spark` | ServiceAccount, Role, RoleBinding | lets Spark drivers start executor pods |

Schema Registry and ksqlDB wait for Kafka before starting, and the datanode waits for the
namenode, so a cold start doesn't crash-loop. Every container runs as a non-root user, has CPU
and memory limits, and has health checks.

Only pods in `bd-gr-05` can use these: Kafka hands out the short name `kafka:9092`, which other
namespaces can't resolve. From a laptop, `kubectl port-forward svc/<name> <port>` works for the
REST APIs, Redis, Cassandra and the HDFS web UI, but not for Kafka or HDFS file reads and writes.

Image tags repeat `versions.env`; change both together.

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

## Moving from the first version (once)

The first version used other names and labels, and Kubernetes can't change those on running
objects. Delete the old objects once, then start the new ones:

```bash
source infra/k8s/cluster/env.sh
kubectl delete deploy,svc,configmap -l app.kubernetes.io/part-of=port-congestion
make k8s-up
```

The `-l` only matches objects from the first version, all of them in `bd-gr-05`.
