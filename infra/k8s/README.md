# Kubernetes (course cluster)

Our stack on the university's shared MicroK8s cluster, in our namespace `bd-gr-05`. How to get
access and the rules for working on a shared cluster are in [cluster/README.md](cluster/README.md).

Plain manifests only: no operators, no CRDs, no Helm charts, nothing outside our namespace (see
decision 10 in `docs/decisions.md`). Services keep their compose names, so connector configs and
app settings that say `kafka:29092`, `schema-registry:8081` or `redis:6379` work unchanged.

## Deploy

```bash
make k8s-diff     # what would change on the cluster
make k8s-apply    # apply it
```

Both go through [deploy.sh](deploy.sh), which refuses to run if any object is cluster-wide or
outside `bd-gr-05`. Add new manifests to [kustomization.yaml](kustomization.yaml).

Check on it:

```bash
source infra/k8s/cluster/env.sh
kubectl get pods                         # all Running / Completed
kubectl logs job/kafka-topics            # ends with the topic list
kubectl port-forward svc/schema-registry 8081   # then http://localhost:8081/subjects
```

## What runs here

| File | What | Compose equivalent |
|---|---|---|
| [kafka.yaml](kafka.yaml) | Kafka, 1 broker, KRaft, 10 Gi | `kafka` |
| [kafka-topics.yaml](kafka-topics.yaml) | Job that creates the topics | `scripts/create_topics.sh` |
| [schema-registry.yaml](schema-registry.yaml) | Schema Registry | `schema-registry` |
| [redis.yaml](redis.yaml) | Redis with keyspace events, 2 Gi | `redis` |

Image tags repeat `versions.env`; change both together. The topic list repeats
`scripts/create_topics.sh`.

## Connecting from other namespaces

Anything in the cluster can reach our services by their full name (there are no network
policies). Inside `bd-gr-05` the short names (`kafka:29092`) work as well.

| Service | Address from any namespace |
|---|---|
| Kafka bootstrap | `kafka.bd-gr-05.svc.cluster.local:29092` (plaintext, no auth) |
| Schema Registry | `http://schema-registry.bd-gr-05.svc.cluster.local:8081` |
| Redis | `redis.bd-gr-05.svc.cluster.local:6379` |
| HDFS | not deployed yet; will be `hdfs://namenode.bd-gr-05.svc.cluster.local:8020` |

From a laptop, `kubectl port-forward` works for Schema Registry and Redis but not for Kafka,
because the broker sends clients its in-cluster name. Run Kafka clients inside the cluster.

## Still to do, roughly in this order

1. Cassandra (StatefulSet) and a Job that applies `cassandra/schema.cql`.
2. HDFS namenode and datanode, and the folder-creating Job.
3. Images for our code (`connect/`, `services/*`, `spark/`) pushed to a registry the cluster can
   pull from, then Kafka Connect, ksqlDB, the producer, API and dashboard.
4. Secrets (`AISSTREAM_API_KEY`, `DMI_API_KEY`, `ALERT_WEBHOOK_URL`) as a Secret created by hand
   with `kubectl create secret`, never committed.
5. Spark jobs as CronJobs.
6. ServiceMonitors labelled `release: kube-prom-stack`, so the cluster's shared Prometheus
   scrapes us.
