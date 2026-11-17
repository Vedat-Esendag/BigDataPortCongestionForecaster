# Spark milestones

Owner: Vedat. Branch: `Spark-Vedat`. Nothing goes to `main` until the PR at the end.

Each milestone: plan, implement, show real output, review, commit.

| Milestone | What | Status |
|---|---|---|
| S1a | Fix the Spark image crash (Python 3.10) | Done, commit `43f365b` |
| S1b | Spark running as pods on local Kubernetes (OrbStack) | Done, commit `ce76738` |
| S2 | Kafka-to-HDFS streaming job | Done, commit `0ea7e5a` |
| S3a | School setup built and rehearsed locally | Done |
| S3b | Run on the school cluster | Blocked |
| S4 | Cleaning (F4): raw to standardized and quarantine | Planned |
| S5 | DMA history into the lake, filtered to Esbjerg | Planned |
| PR | One pull request to `main` | After S3a and S4, or once S3b runs |

## S1a: Fix the Spark image (done)

The image runs Python 3.10, and the code used `datetime.UTC` (3.11+), so `live_job` and `train_eta` crashed on start.

- Replaced `datetime.UTC` with `timezone.utc`
- `spark/` now requires Python >=3.10, with a ruff rule to keep it that way
- Verified: `make test` passes, the image builds, all four jobs print `--help` inside the image

## S1b: Spark as pods on OrbStack (done)

Added:
- `infra/k8s/overlays/local/`: local-only overlay (namespace, Spark RBAC from base, shared `spark-defaults.conf`, two Jobs, README)
- `spark/jobs/smoke_count.py`: counts the AIS fixture rows inside the image
- `spark/tests/test_smoke_count.py`

Verified:
- A submitter pod, a driver pod and 2 executor pods ran for each job
- SparkPi printed `Pi is roughly 3.1435...`, smoke_count printed `rows 20`, `executors 2`
- Both driver pods ended `Succeeded`
- The school overlay renders byte-identical before and after
- `make test` and `make lint` pass

Learned on the way:
- The image's user is `spark` by name; Kubernetes needs the number, so the Jobs set `runAsUser: 185`
- Spark 4.2 creates a NetworkPolicy per app, which the `spark` Role does not allow. Skipped locally with `spark.kubernetes.driver.pod.excludedFeatureSteps`
- Finished driver and executor pods stay as `Completed`; deleting the driver pod removes its executors

## S2: Kafka-to-HDFS job (done)

Decided:
- Spark writes the live AIS topics to HDFS, not the Kafka Connect HDFS sink
- The Kafka and Avro jars are baked into the Spark image, so jobs need no `--packages` and no
  internet at run time
- Output `/lake/raw/<topic with dots as underscores>/day=yyyy-MM-dd/`, Parquet, every record kept
  (bad rows and duplicates too; cleaning is a later step). Checkpoint
  `/checkpoints/kafka_to_lake/<topic>`, outside the lake

Added:
- `spark/streaming/kafka_to_lake.py`: one streaming query per topic (`--topic`), trigger every
  minute by default (`--trigger`), starting offsets `earliest`. Keeps `kafka_partition`,
  `kafka_offset` and `kafka_timestamp`. `day` is the UTC day of `event_time`, or of the Kafka
  timestamp when a record could not be decoded
- `spark/Dockerfile`: resolves `SPARK_KAFKA_PACKAGE` and `SPARK_AVRO_PACKAGE` at build time and
  adds the 5 jars Spark does not ship (`spark-sql-kafka`, `spark-token-provider-kafka`,
  `kafka-clients`, `commons-pool2`, `spark-avro`)
- `scripts/send_ais_fixtures.py`: test producer (confluent-kafka `AvroSerializer`, the library the
  real producer uses), plus `spark/tests/fixtures/ais_static_sample.csv`
- `spark/tests/test_kafka_to_lake.py`, `make spark-lake TOPIC=...`, how-to in `spark/README.md`

Verified on docker-compose:
- 20 positions and 6 static records sent, 20 and 6 rows read back from Parquet, in
  `day=2026-10-01` (positions) and `day=2026-09-30` / `day=2026-10-01` (static). The bad rows
  and the duplicate are in
- Restart test: stop both jobs, send everything again, restart. 40 and 12 rows, every
  (partition, offset) exactly once
- `live_job.py` and `kafka_to_lake.py` start in the image with no `--packages` and no network

Learned on the way (compose, fixed):
- The namenode never formatted: the image formats only if `/hadoop/dfs/name` does not exist, and
  the volume was mounted on that path. Now mounted at `/hadoop/dfs`
- The Kafka healthcheck asked `localhost:29092`, but that listener is bound to `kafka`, so Kafka
  was never healthy and Schema Registry never started
- Spark wrote HDFS files with replication 3 (the client default) on a single datanode, so every
  block was under-replicated. Spark jobs now get `spark.hadoop.dfs.replication=1` from where the
  environment is configured: `SPARK_COMPOSE_CONF` in the Makefile and
  `infra/k8s/overlays/local/spark-defaults.conf`. **The school setup needs the same setting**
  (its HDFS uses `dfs.replication` 1): done in S3a, `infra/k8s/spark/school/spark-defaults.conf`

Open (not fixed, for the owners):
- The Schema Registry healthcheck uses `curl`, which the image does not have, so it always shows
  `unhealthy` although the registry works. Services with `required: false` start anyway
- `connect/hdfs-parquet-sink.json` still lists `ais.position` and `ais.static`. If Connect runs,
  the lake gets a second raw copy (`/lake/raw/ais.position/dt=.../hr=...`). Remove them there;
  `docs/architecture.md` still says the sink writes these topics
- Spark reads `day` back as a `date` (partition type inference); the folder value is text. Set
  `spark.sql.sources.partitionColumnTypeInference.enabled=false` to read it as a string

### For the report: Avro on Kafka to Parquet on HDFS

The same records change format on the way into the lake:

| | Kafka | HDFS raw zone |
|---|---|---|
| Format | Avro, one binary record per message, Confluent framing (magic byte + 4-byte schema id) | Parquet, columnar files, snappy compressed |
| Schema | in Schema Registry, referenced by id | stored in each Parquet file |
| Organised by | topic, partition, offset | topic folder, `day=` partitions |
| Good for | streaming one record at a time, low latency | scanning a few columns over many rows (training, analytics) |

Avro suits Kafka because it is compact per record and the schema can evolve through the
registry. Parquet suits the lake because jobs read a few columns of many rows, and columnar files
compress well and skip unread columns. The Kafka partition and offset are kept as columns, so any
lake row can be traced back to its message.

Why Spark rather than the Kafka Connect HDFS sink:
- One decoder: the job uses `common/avro_io.py`, the same code the live job uses, so the lake and
  the live job see the same values
- Exactly-once into the lake: the checkpoint plus the file sink's `_spark_metadata` log mean a
  restart neither loses nor repeats records (shown above)
- Our own layout: `day` from the event time in UTC, and the Kafka metadata as columns
- Licence: the HDFS 2 sink is under the Confluent Community License (docs/connectors.md:
  "free to use, but not to offer as a competing hosted service"); Spark is Apache-2.0. For a
  course project this does not decide anything

Trade-offs:
- A long-running Spark job to keep alive per topic, where Connect would run it as a managed task
- Many small files: a file per Kafka partition and day on every trigger. A compaction job (or a
  longer trigger) is needed for real volumes
- Schema evolution is not handled: `avro_io` decodes with the `.avsc` file, not with the schema id
  in each message (the known limit in `avro_io.py`)
- Undecodable messages become all-null rows (with partition, offset and timestamp) instead of
  going to a dead-letter topic

## S3: School cluster

S3 is split in two: S3a builds and rehearses everything locally, S3b runs it on the school
cluster once the other parts are in place.

### What the school cluster looks like (checked 16 Nov, read-only)

- Reachable with the team kubeconfig, context `bd-gr-05-context`
- Running in `bd-gr-05` since 11 Nov: Kafka, Schema Registry, HDFS namenode and datanode,
  Cassandra, Redis, ksqlDB. The `spark` ServiceAccount, Role and RoleBinding are deployed
- Our token can create pods and NetworkPolicies
- One node (`leftover`), amd64, Kubernetes v1.35.6. Pod-level failure tests work; a machine
  failure cannot be shown
- Every image comes from docker.io, so the cluster can pull public images. No registry of its own
- No persistent volumes: Kafka, HDFS and Cassandra lose their data when their pod restarts. The
  overlay comment names storage class `hostpath-custom` for when they get one
- Per `infra/k8s/README.md`: no topics, schemas or HDFS folders yet, and Kafka and HDFS cannot be
  reached from a laptop (port-forward only works for the REST APIs, Redis, Cassandra, HDFS web UI)
- `env.sh` fails in zsh (`BASH_SOURCE` is empty); `export KUBECONFIG=$PWD/infra/k8s/cluster/kubeconfig.yaml` works.
  Local kubectl is v1.37, two minor versions ahead of the cluster

### S3a: build and rehearse locally (done)

Scope:
- One Spark image for linux/amd64 and linux/arm64, tagged with the git commit. Built locally
  only, not pushed: the registry is not chosen yet (S3b)
- School Spark settings and Jobs (smoke count, recorder) in their own folder, rendering in
  namespace `bd-gr-05` with the school addresses (`kafka:9092`, `hdfs://hdfs-namenode:8020`)
  and `spark.hadoop.dfs.replication=1`
- The recorder kept running by Kubernetes: if its driver dies, it comes back on its own and
  continues from the checkpoint
- A test sender that runs as a pod inside the cluster, since Kafka is not reachable from a laptop
- A full rehearsal on OrbStack with the base Kafka, Schema Registry and HDFS from `infra/k8s/base`,
  using the same Spark manifests as the school, only the image source differs

Not included: changes to other people's parts (`base/`, `overlays/school/kustomization.yaml`,
`deploy.sh`, `infra/k8s/README.md`), anything on the school cluster.

Done when:
- The image manifest lists both platforms, and the amd64 variant runs the smoke count (`rows 20`)
- The school Spark folder renders, everything in `bd-gr-05`; the school overlay render is unchanged
- On OrbStack: smoke job `Succeeded` with `rows 20`; the recorder writes the fixtures to HDFS;
  after its driver is deleted it comes back by itself, and after a second send the totals are
  exactly 2x with every offset once
- `make test` and `make lint` pass

Decided:
- The recorder is a **StatefulSet with one replica**, one per topic, running the driver in the
  pod (client mode) with one executor pod. Kubernetes restarts it when the driver dies or is
  deleted. A StatefulSet starts the replacement only after the old pod is gone, so two drivers
  never write the same checkpoint (a Deployment can briefly run both). Not chosen: a Job running
  spark-submit in cluster mode (spark-submit usually exits 0 when the driver pod is deleted, so
  the Job does not retry), `--master local[2]` in one pod (smaller, but no executors on
  Kubernetes), the Spark Operator (needs cluster-wide CRDs, which our token cannot install)
- The test sender runs in the Spark image, so there is only one image to build and push:
  `confluent-kafka[avro,schemaregistry]` is installed there in its own Dockerfile step, only for
  the sender (same version as `services/ais-producer/uv.lock`). Trade-off: about 20 Python
  packages in the Spark image that no Spark job uses, against a second image with its own build,
  tag and registry entry
- One place for the image: `images:` in the kustomization. A `replacements` block copies it into
  each Spark pod's `SPARK_IMAGE`, which is passed as `spark.kubernetes.container.image`, so the
  executors always run the driver's image. An overlay that changes the image must repeat the
  block (replacements run before a parent's `images`)

Added:
- `make spark-image`: `docker buildx build --platform linux/amd64,linux/arm64 --load`, tagged
  `spark-jobs:<git short sha>`, with `-dirty` and a warning when the tree has uncommitted
  changes. Never pushes. Docker uses the containerd image store, so both platforms load without
  a registry
- `infra/k8s/spark/` (new folder next to `base/` and `overlays/`, how-to in its `README.md`):
  - `school/`: `spark-defaults.conf` (cluster mode, 512m pods, one executor, replication 1),
    `school.env` (the addresses) and the two recorder StatefulSets with headless Services. Safe
    to deploy as a whole
  - `school/one-off/`: the test Jobs `smoke-count`, `send-fixtures` and `lake-check`, applied one
    at a time by hand. Kept out of `school/` so a deploy never runs them: `send-fixtures` would
    put test rows into the real topics and from there into the lake
  - `school/image/`: the image for both, as a kustomize Component. Two open settings are marked
    in the files: **IMAGE** there (registry placeholder `REGISTRY_NOT_CHOSEN/spark-jobs`, which
    fails at once) and **NETWORKPOLICY** in `spark-defaults.conf` (one line that skips the
    NetworkPolicy step)
  - `rehearsal/`: OrbStack only. Namespace, `base/{kafka,schema-registry,hdfs,spark}`,
    `../school` and `../school/one-off`, with only the image mapped to the local
    `spark-jobs:rehearsal`. Topics, schemas
    and HDFS folders are created by rehearsal-only one-off commands in the README
- `spark/jobs/lake_counts.py` (the `lake-check` Job): rows, duplicates and offset gaps per
  dataset, since HDFS cannot be read from a laptop. `spark/tests/test_lake_counts.py`,
  `spark/tests/test_k8s_school.py` (namespace, addresses, replication, the two recorders differ
  only in the topic) and a version check for the confluent-kafka pin

Verified on OrbStack (16 and 17 Nov):
- `spark-jobs:0295117-dirty` lists `linux/amd64` and `linux/arm64`. Run as amd64: Python
  3.10.12, smoke count `rows 20`
- The school folder renders with every object in `bd-gr-05` and no Jobs. The rehearsal render
  differs from school plus one-off only in the image (8 lines on each side: 5 `image:` and 3
  `SPARK_IMAGE`). The school overlay render is byte-identical before and after
- `apache/hadoop:3.5.0` (amd64 only) runs on OrbStack Kubernetes (emulated, `uname -m` x86_64)
- Smoke job: driver `Succeeded`, `rows 20`, `executors 2`
- Sender pod sent 20 and 6; the recorders wrote them (`lake-check`: 20 and 6 rows, duplicates 0,
  gaps 0). Both recorders deleted: new driver pods within 2 s and new executors 5 s later,
  `Resuming at batch 2` from the checkpoint. After a second send: 40 and 12 rows, every
  (partition, offset) once, no gaps. `fsck`: 0 under-replicated blocks
- Both recorders (about 3.3 GiB of requests) fit next to the base services on the laptop, but
  not together with the smoke job as well (its executors stay `Pending`, `Insufficient memory`).
  Rerun from `school/one-off` with the recorders scaled to 0: `rows 20`, and the recorders then
  continued at batch 3

Learned on the way:
- Without persistent storage, a restart of the cluster (here OrbStack, during a pause) wipes
  Kafka's topics and HDFS. The recorders then crash-loop with `Permission denied ... inode="/"`
  (the `/checkpoints` folder is gone) instead of writing elsewhere, and recover once the folders
  exist again. On the school cluster the same happens when the HDFS pods restart
- When a client-mode driver stops, Spark tries to delete services by label and gets
  `services is forbidden` (the `spark` Role has no `deletecollection`, on purpose). Harmless:
  the executor pods and their ConfigMaps are owned by the driver pod and are removed with it
- The S1b `smoke-count` Job in `overlays/local` has the same name as the school one; delete the
  old Job before running the rehearsal in the same namespace

### S3b: run on the school cluster (blocked)

Waiting on:
- Gerardas: the registry (OK for public images on ghcr.io?) and the image name, `networkpolicies`
  in the `spark` Role (then delete the NETWORKPOLICY line), and wiring `infra/k8s/spark/school`
  into `deploy.sh`
- Oskar: topics `ais.position` and `ais.static` (3 partitions) on the school Kafka, schemas
  registered in the school Schema Registry
- Ahmed: HDFS folders `/lake/...`, `/models/eta` and `/checkpoints`, owned by user `spark`, and
  persistent volumes

Then: push `spark-jobs:<sha>`, set the registry and tag in `school/image/kustomization.yaml`,
`deploy.sh diff`, `apply`, smoke job, recorders, sender, `lake-check`, restart test, with the same
checks as S3a.

Known risk until Kafka has persistent storage: if Kafka loses its data but HDFS keeps the
recorder's checkpoint, the recorder stops with a data-loss error instead of skipping the gap.

## S4: Cleaning (F4), planned

Scope:
- Implement `clean_positions` and `clean_static` in `spark/common/clean.py` so the 4 strict
  xfail tests in `spark/tests/test_clean.py` pass (duplicates removed, impossible speed and bad
  MMSI rejected, clean plus rejected rows add up), and cover the other rules listed in
  `clean.py` (positions outside the globe, "not available" codes) with new tests
- A job that reads `/lake/raw` and writes clean rows to `/lake/standardized` and rejected rows,
  with their reason, to `/lake/quarantine`
- The live job uses the same functions, so live and history are cleaned the same way

Not included: port events (F5), features (F6), the model.

Done when:
- The 4 xfail tests pass as normal tests
- Running the job on the fixture gives the expected clean and rejected counts, with each reject
  reason visible
- `make test` and `make lint` pass

## S5: DMA history into the lake, planned

Scope: run the recorder on `ais.history.position` and `ais.history.static` with
`--trigger available-now`, plus a filter to the Esbjerg bounding box, so the lake does not store
all Danish waters.

Depends on: the DMA import (Kafka Connect FilePulse and the ksqlDB split) running, and the group
agreeing where the area filter belongs.
