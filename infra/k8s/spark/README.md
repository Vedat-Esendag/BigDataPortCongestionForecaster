# Spark on the school cluster, and its rehearsal on OrbStack

| Folder | What |
|---|---|
| `school/` | Spark settings and the two recorders, namespace `bd-gr-05`. Safe to deploy as a whole |
| `school/one-off/` | The smoke job, the test sender and the lake check: test Jobs, applied one at a time by hand |
| `school/image/` | The image for both (a kustomize Component), with the IMAGE setting |
| `rehearsal/` | **Local OrbStack only.** The base Kafka, Schema Registry, HDFS and Spark RBAC, plus `school/` and `school/one-off/` with a local image |

`school/` adds only Spark objects. Kafka, Schema Registry, HDFS and the `spark` service account
come from `overlays/school`. `school/` is not part of `deploy.sh` yet, and nothing here has been
applied to the school cluster (S3b in `docs/spark-milestones.md`).

The one-off Jobs are kept out of `school/` so that deploying it never runs them: `send-fixtures`
would put test rows into the real `ais.position` and `ais.static` topics, and from there into the
lake. On the school cluster, run one with
`kubectl apply -k infra/k8s/spark/school/one-off -l one-off=<name>`.

## What is in `school/` and `school/one-off/`

| Object | Kind | What it does |
|---|---|---|
| `spark-school-conf` | ConfigMap | `spark-defaults.conf`: master, namespace, sizes, `dfs.replication 1` |
| `spark-school-env` | ConfigMap | `kafka:9092`, `http://schema-registry:8081`, `hdfs://hdfs-namenode:8020` |
| `kafka-to-lake-ais-position`, `kafka-to-lake-ais-static` | StatefulSet + headless Service | the recorders (`spark/streaming/kafka_to_lake.py`), running all the time |
| `smoke-count` | Job, in `one-off/` | counts the fixture in the image, prints `rows 20` |
| `send-fixtures` | Job, in `one-off/` | test sender: 20 rows to `ais.position`, 6 to `ais.static` |
| `lake-check` | Job, in `one-off/` | rows, duplicates and offset gaps per dataset in `/lake/raw` |

Two settings are open and marked in the files:
- **IMAGE** (`school/image/kustomization.yaml`, one place for both folders): the registry is not
  chosen, so the image is a placeholder that fails at once. `make spark-image` builds
  `spark-jobs:<git sha>`. `make spark-push` builds the linux/amd64 variant and pushes it to
  `$SPARK_REGISTRY/spark-jobs:<git sha>` (default `ghcr.io/vedat-esendag`); it refuses a tree
  with uncommitted or untracked files. `make images-push` pushes it together with our other
  images, with the same tag (`infra/k8s/README.md`, Images).
- **NETWORKPOLICY** (`school/spark-defaults.conf`): one line skips the NetworkPolicy that Spark
  4.2 creates in cluster mode, because the `spark` Role may not create it.

### Why the recorders are StatefulSets

The driver runs inside the StatefulSet pod (client mode) and starts one executor pod. When the
driver pod dies or is deleted, Kubernetes starts a new one by itself, and it continues from the
checkpoint in HDFS, so nothing is lost or written twice. A StatefulSet with one replica starts
the new pod only after the old one is gone, so two drivers never write the same checkpoint (a
Deployment can briefly run both). The executor pods are owned by the driver pod and are removed
with it. A failed streaming query ends the process, and the container restarts.

## Rehearsal on OrbStack

**Every command passes `--context orbstack`.** Never use `cluster/env.sh`,
`cluster/kubeconfig.yaml`, `deploy.sh` or `check.sh` with this folder. Check first:

```bash
kubectl config get-contexts
```

All commands from the repo root. About 7 GiB of memory for everything; stop the compose stack
first (`make down`).

### 1. Image

```bash
make spark-image
```

It prints the tag, `spark-jobs:<git sha>` (with `-dirty` when there are uncommitted changes).
Give that image the name the rehearsal uses:

```bash
docker tag spark-jobs:<tag> spark-jobs:rehearsal
```

### 2. Kafka, Schema Registry, HDFS

Everything except the Spark objects:

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l 'layer!=spark-jobs'
```

```bash
kubectl --context orbstack -n bd-gr-05 rollout status statefulset/kafka statefulset/hdfs-namenode statefulset/hdfs-datanode deployment/schema-registry --timeout=5m
```

### 3. Topics, schemas, HDFS folders (rehearsal only)

On the school cluster these belong to Oskar (topics, schemas) and Ahmed (HDFS folders). Here
they are one-off commands, run again after a Kafka or HDFS pod restarts (no persistent storage).

```bash
for t in ais.position ais.static; do kubectl --context orbstack -n bd-gr-05 exec kafka-0 -- kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1; done
```

In a second terminal, keep a port-forward to Schema Registry open:

```bash
kubectl --context orbstack -n bd-gr-05 port-forward svc/schema-registry 18081:8081
```

```bash
python3 scripts/register_schemas.py --url http://localhost:18081
```

Spark runs as user `spark`, so it must own its folders:

```bash
kubectl --context orbstack -n bd-gr-05 exec hdfs-namenode-0 -- sh -c 'hdfs dfs -mkdir -p /lake/raw /checkpoints && hdfs dfs -chown -R spark /lake /checkpoints'
```

### 4. Spark settings, then the smoke job

The two ConfigMaps every Spark pod reads:

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l app.kubernetes.io/component=spark-settings
```

If you ran `overlays/local` in this namespace before, delete its Job of the same name first
(`kubectl --context orbstack -n bd-gr-05 delete job smoke-count`). Then:

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l one-off=smoke-count
```

On a laptop the smoke job (about 2.6 GiB) does not fit next to both running recorders: its
executors stay `Pending` (`Insufficient memory`). Run it before step 5, or first scale the
recorders to 0, and afterwards back to 1 (they continue from their checkpoints):

```bash
kubectl --context orbstack -n bd-gr-05 scale statefulset -l app.kubernetes.io/name=kafka-to-lake --replicas=0
```

The driver pod must end `Succeeded`, and its log shows `rows 20`:

```bash
kubectl --context orbstack -n bd-gr-05 get pods -l spark-role=driver,spark-job=smoke-count
```

```bash
kubectl --context orbstack -n bd-gr-05 logs -l spark-role=driver,spark-job=smoke-count --tail=-1 | grep -E '^(rows|master|executors) '
```

### 5. Recorders

Both recorders (StatefulSets and their Services):

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l app.kubernetes.io/name=kafka-to-lake
```

If memory is short, apply one recorder with `-l spark-job=kafka-to-lake-ais-position`. Pods: `kafka-to-lake-ais-position-0` (driver) and `kafka-to-lake-ais-position-…-exec-1`.

```bash
kubectl --context orbstack -n bd-gr-05 logs kafka-to-lake-ais-position-0 | grep kafka_to_lake
```

### 6. Send the fixtures, check the lake

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l one-off=send-fixtures
```

```bash
kubectl --context orbstack -n bd-gr-05 logs job/send-fixtures
```

After the next trigger (up to a minute):

```bash
kubectl --context orbstack -n bd-gr-05 exec hdfs-namenode-0 -- hdfs dfs -ls -R /lake/raw
```

```bash
kubectl --context orbstack apply -k infra/k8s/spark/rehearsal -l one-off=lake-check
```

```bash
kubectl --context orbstack -n bd-gr-05 logs job/lake-check | grep -E '^ais_'
```

`duplicates 0` and `gaps 0` mean every Kafka record is in the lake exactly once.

### Run a one-off Job again

A Job runs once. Delete it, then apply it again with the same command as above:

```bash
kubectl --context orbstack -n bd-gr-05 delete job send-fixtures
```

### Restart test

Delete the driver pod of a running recorder. The StatefulSet starts a new one, which continues
from the checkpoint. Send the fixtures again; `lake-check` then shows exactly twice the rows,
still with `duplicates 0`.

```bash
kubectl --context orbstack -n bd-gr-05 delete pod kafka-to-lake-ais-position-0
```

```bash
kubectl --context orbstack -n bd-gr-05 get pods -o wide -w
```

### Clean up

Stop the recorders and remove the Spark objects (the driver pods of `smoke-count` stay behind
as `Completed`; delete them with `-l spark-role=driver,spark-job=smoke-count`):

```bash
kubectl --context orbstack delete -k infra/k8s/spark/rehearsal -l layer=spark-jobs
```

Remove everything, the `bd-gr-05` namespace included (also what `overlays/local` created there):

```bash
kubectl --context orbstack delete -k infra/k8s/spark/rehearsal
```
