# Spark milestones

Owner: Vedat. Branch: `Spark-Vedat`. Nothing goes to `main` until the PR at the end.

Each milestone: plan, implement, show real output, review, commit.

| Milestone | What | Status |
|---|---|---|
| S1a | Fix the Spark image crash (Python 3.10) | Done, commit `43f365b` |
| S1b | Spark running as pods on local Kubernetes (OrbStack) | Done, commit `ce76738` |
| S2 | Kafka-to-HDFS streaming job | Done, commit `0ea7e5a` |
| S3a | School setup built and rehearsed locally | Done |
| S3b | Run on the school cluster | Running; restart test open |
| S4 | Cleaning (F4): raw to standardized and quarantine | Planned |
| S5 | DMA history into the lake, filtered to Esbjerg | Planned |
| R1 | The October Esbjerg waiting analysis rebuilt in Spark, compared event by event | Done, not committed |
| R2 | Method fixes: repeat visits, ETA at a fixed lead time, the R1 decisions to review | Planned |
| R3 | Fresh full DMA days, cut to Esbjerg in Spark, then the R1 pipeline on them | Planned |
| D1 | DMA days into the lake: loader and cleaning job (#10) | Built, one day proven locally (27 Nov) |
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

Re-run on OrbStack (23 Nov, after merging `origin/main`, commit `20974d6`):
- Image `spark-jobs:20974d6` (clean tree, `linux/amd64` and `linux/arm64`), tagged
  `spark-jobs:rehearsal`
- Fresh setup first (see the root cause below): topics `ais.position` and `ais.static` with 3
  partitions, 12 schemas registered, `/lake/raw` and `/checkpoints` owned by `spark`
- Smoke job: 1 driver and 2 executor pods, driver `Succeeded`, `rows 20`, `executors 2`
- Recorders: 1 driver and 1 executor pod each, 0 restarts. After `send-fixtures` (20 and 6),
  `lake-check`: 20 and 6 rows, duplicates 0, gaps 0 in every partition
- Restart test: `kafka-to-lake-ais-position-0` deleted at 10:56:11; the new driver pod ran at
  10:56:15 and its executor at 10:56:19. It logged `Resuming at batch 2` with committed offsets
  `{"0":8,"1":4,"2":8}`, under the same query id. The old executor went with the old driver pod
- Second send: 40 and 12 rows (16 / 8 / 16 per partition), every (partition, offset) once, gaps 0.
  `hdfs fsck /lake`: HEALTHY, 0 under-replicated, missing or corrupt blocks
- Afterwards both recorders were scaled to 0 (see the root cause)

Root cause of the crash-loop found before the re-run: the HDFS and Kafka pods in the OrbStack
rehearsal have no persistent volumes, so an OrbStack restart wipes HDFS (`/lake`,
`/checkpoints`) and the Kafka topics. Here HDFS went down with OrbStack on 21 Nov and came back
empty on 23 Nov. Each recorder then starts as user `spark`, cannot create
`/checkpoints/kafka_to_lake/<topic>` (`Permission denied: user=spark, access=WRITE,
inode="/":hadoop:supergroup:drwxr-xr-x`), and exits; the StatefulSet restarts it (22 times
each). It is fixed by persistent volumes (Ahmed's PVCs on `HDFS_Ahmed`). Until those are
merged, after any OrbStack restart redo step 3 of the rehearsal README (topics, schemas, HDFS
folders) before starting the recorders, or keep them scaled to 0.

Learned on the way:
- Without persistent storage, a restart of the cluster (here OrbStack, during a pause) wipes
  Kafka's topics and HDFS. The recorders then crash-loop with `Permission denied ... inode="/"`
  (the `/checkpoints` folder is gone) instead of writing elsewhere, and recover once the folders
  exist again. On the school cluster the same happens when the HDFS pods restart
- When a client-mode driver stops, Spark tries to delete services by label and gets
  `services is forbidden` (the `spark` Role has no `deletecollection`, on purpose). Harmless
  when the driver pod is deleted: the executor pods and their ConfigMaps are owned by it and go
  with it. Not in a crash-loop: the container restarts inside the same pod, so each attempt
  leaves a `Completed` executor pod (46 by 23 Nov). Deleting or scaling down the driver pod
  removes them
- The S1b `smoke-count` Job in `overlays/local` has the same name as the school one; delete the
  old Job before running the rehearsal in the same namespace

### S3b: run on the school cluster (running, restart test open)

Image:
- `ghcr.io/vedat-esendag/spark-jobs:dede587`, public, linux/amd64, pushed with `make images-push`
  (digest `sha256:2fb209fedef714bc58ddc2d4299f6c9628660ccb9a8d26bbef2c693cfc14d748`). Mapped
  in `school/image/kustomization.yaml`; pulls anonymously, so no pull secret

On the school cluster (`bd-gr-05`):
- Smoke job: `rows 20`
- Both recorders (`kafka-to-lake-ais-position-0`, `kafka-to-lake-ais-static-0`) running since
  23 Nov (14:46 and 14:48 UTC) on `spark-jobs:dede587`, one executor each, 0 restarts
- After `send-fixtures`, `lake-check`: 20 and 6 rows, duplicates 0
- Re-checked on 25 Nov (read-only): topic end offsets `ais.position` 8 / 4 / 8 and `ais.static`
  3 / 1 / 2, the same as the last checkpoint batch (`{"0":8,"1":4,"2":8}` and
  `{"0":3,"1":1,"2":2}`); the recorders still log a micro-batch every minute, no errors

Open:
- Restart test (delete the `ais.position` driver pod, resume from the checkpoint, second send,
  expect 40 and 12 rows with every (partition, offset) once). Blocked since 25 Nov: the node's
  root disk is 99 % full (DiskPressure), so no new pod can be scheduled, and Schema Registry,
  which `send-fixtures` needs, is down. To run once the disk is freed
- NETWORKPOLICY line in `school/spark-defaults.conf` (Gerardas: `networkpolicies` in the
  `spark` Role), and wiring `infra/k8s/spark/school` into `deploy.sh`

Known risk: if Kafka loses its data but HDFS keeps the recorder's checkpoint, the recorder stops
with a data-loss error instead of skipping the gap.

Open issues (local OrbStack rehearsal):
- OrbStack rehearsal recorders (kafka-to-lake-ais-position-0 / -ais-static-0) crash-loop after
  OrbStack restart. Cause found on 23 Nov: no persistent volumes, so HDFS and the topics are
  wiped (see the S3a re-run above). Open until Ahmed's PVCs are merged; recorders scaled to 0

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

## R1: the October Esbjerg waiting analysis in Spark (done, not committed)

A second, independent implementation of the October pandas analysis (`check_datasets.py`:
`load_port_class_a`, `port_waiting`; `year_analysis.py`: the large-ship rule; `make_figures.py`:
the ETA table), on the same 84 days (`year_comparison.json["days"]`, days 8-14 of each month,
2025-11 to 2026-10). Neither side was assumed correct; any difference was to be investigated per
ship, and no rule was changed to get a match. Wind is out of scope.

Added:
- `spark/common/esbjerg_r1.py`: every rule, each citing its line in the October code
  (`cd:` check_datasets.py, `ya:` year_analysis.py, `mf:` make_figures.py). DataFrame and window
  functions only (lag, running sums for run ids, grouped aggregates), no UDFs
- `spark/jobs/ingest_portarea.py` (R1a): `portarea_<day>.csv.gz` to
  `/lake/standardized/ais_positions/source=portarea/day=<day>/`, explicit schema, rejects
  (missing or unparsable ts/lat/lon, with `reject_reason`) to
  `/lake/quarantine/ais_positions/source=portarea/`, manifest per day in
  `/lake/standardized/_manifests/portarea/<run>/` (rows in, out, rejected by reason, Class A,
  rows whose ts date is not the file day). For each selected day both output partitions are
  deleted and rewritten, so a day whose new result is empty (no rejects any more) keeps no stale
  rows; days that were not selected are left as they are
- `spark/jobs/check_source_order.py`: SHA-256 of all 18 columns of every row in file order:
  raw text (plain Python gzip + csv vs the Spark reader) and parsed values (a plain Python parse
  vs the Spark parse vs the lake read back), all formatted the same way (null as `\N`)
- `spark/jobs/esbjerg_arrivals.py` (R1b): `/lake/curated/esbjerg_arrivals`, `esbjerg_stops`,
  `esbjerg_eta_at_entry`, and a stage-count manifest in `/lake/curated/_manifests/esbjerg_r1/`
- `scripts/compare_r1.py` (R1c): event-level comparison (null-safe joins, keys checked unique on
  both sides), the summary numbers side by side, the Nordby count, the first mismatches with a
  track preview, `--inspect BLOCK:MMSI` for the full stop runs of one ship. Exits 0 only when
  every event, field and summary comparison matches (and `year_comparison.json` on the 84-day
  run), 1 otherwise, after printing all diagnostics
- `spark/tests/test_esbjerg_r1.py`: 27 tests on hand-made tracks (the brief's list, plus the
  500k-chunk thinning, moored share, null nav in the shares, ship-type mode, blocks, large ship,
  ingest rejects, ETA from quarantined rows and arrivals without an ETA, a day going from
  rejects to no rejects without stale rows)
- `infra/compose.r1.yml`: mounts `PORTAREA_DIR` read-only at `/data/portarea` in the Spark
  container, only for the `r1-*` targets; the normal stack does not need the folder
- Makefile: `r1-up`, `r1-ingest`, `r1-order`, `r1-arrivals`, `r1-compare` (`R1_BLOCKS=0` for one
  block); `PORTAREA_DIR=` in `.env.example`

Run:

```bash
make r1-up PORTAREA_DIR=/Volumes/LaCie_SSD/big_data_datasets/code/data
```

```bash
make r1-ingest r1-order r1-arrivals r1-compare PORTAREA_DIR=/Volumes/LaCie_SSD/big_data_datasets/code/data R1_DRIVER_MEMORY=4g
```

### The October rules, as ported

| Rule | October line |
|---|---|
| Class A only (`mobile == "Class A"`, exact) | cd:1021 |
| ts parsed with coerce; rows without ts, lat or lon dropped (here: quarantined) | cd:1024-1025 |
| first row per (mmsi, minute) **within each 500,000-line pandas chunk** | cd:1019, 1026 |
| stable sort by (mmsi, ts), first row per (mmsi, ts) | cd:1030-1031 |
| day = ts date; consecutive days share a block (0-11) | cd:1050-1058, 1067-1069 |
| harbour zone 55.440-55.490 N, 8.405-8.490 E, ends inclusive | cd:53, 108, 1070 |
| ignored = inside NORDBY_ZONE and not in the harbour zone | cd:55, 1071-1074 |
| ship type = mode over all loaded days, without null/"Undefined", ties alphabetical | cd:1075-1076 |
| anchored = nav contains "anchor" (lower case); stationary = sog < 0.5 or anchored | cd:1077-1078 |
| wait flag = stationary, outside the zone, not ignored | cd:1079 |
| groups (block, mmsi) in ts order; skip if never inside or first row inside | cd:1082-1086 |
| arrival = first in-zone row; only rows before it are looked at | cd:1087, 1092, 1095 |
| run breaks on a flag change or a gap > 30 min (30 exactly does not) | cd:1081, 1094 |
| stop counts if >= 30 min | cd:1097 |
| stop >= 50 % "Moored" messages: counted as moored elsewhere, not a stop | cd:1100-1102 |
| anchor stop if >= 10 % of its messages are anchored (share of messages) | cd:1099, 1110 |
| large = Cargo/Tanker, or length (null as 0) >= 90 and not a service/passenger type | ya:37, cd:1004-1005 |
| ETA: last Class A ETA message (raw rows) in [arrival - 3 d, arrival], large arrivals only | mf:231-247 |
| ETA kept if \|arrival - ETA\| <= 7 days | mf:256-258 |

"ETA reported at entry" is the last ETA a ship sent before it entered. It is not a forecast.

### Parity decisions

- **Source order.** Each file is read alone with `textFile` (gzip cannot be split, so one
  partition), every raw line is numbered with `zipWithIndex` before parsing (index 0 is the
  header, `line_no = index - 1`), and rows are parsed with `from_csv` so `line_no` stays
  attached. `monotonically_increasing_id` is not used. Checked on two full days, see below
- **Ties.** For equal (mmsi, ts) the first row in file order wins, as in pandas (its multi-column
  sort is stable). After minute thinning no (mmsi, ts) tie is left, so in practice the rule is
  "first row of each minute in file order"
- **Chunks.** The pandas thinning runs inside each 500,000-line chunk. Spark carries
  `chunk = line_no // 500000` and thins per chunk too. No day file has more than 500,000 lines,
  so it changes nothing on this data; a test covers the boundary
- **pandas NA strings.** pandas reads "NA", "None", "null", "n/a" and similar as missing; the
  ingest does the same for string columns, so names and types match
- **ETA input** includes quarantined rows (a row without lat/lon still carries its ETA), and every
  large arrival is kept, with a null ETA when nothing matches. For equal ts with different ETAs
  pandas' pick is undefined (single-column quicksort); Spark takes the last row in file order.
  This case did not occur (0 large arrivals)
- **Medians** use Spark's exact `percentile` (linear interpolation, as pandas)

### Results (compose Spark container, 23 Nov)

Source order, full files (`make r1-order`): SHA-256 over all 18 columns of every data row, in
file order, each value formatted the same way on every side (null as `\N`, floats with Python
`repr`, timestamps `yyyy-mm-dd HH:MM:SS`, fields joined by `\x1f`). Five sides per day: raw text
from plain Python (gzip + csv) and from the Spark reader (textFile + zipWithIndex + from_csv);
parsed values from a plain Python parse, from the Spark parse (`parse_lines`) and from the lake
read back (standardized + quarantine). All five agree:

| Day | Rows (each side) | SHA-256 (all five sides) |
|---|---|---|
| 2025-11-08 | 423,309 | `9b2aa9ba12406519c66d2c8554465a35f3a1346daa79e511d8426443b0ed665b` |
| 2026-10-14 | 370,834 | `3437254f1b991c27bd41db984d2ebb38b6ad4c816ca5cf660e8debe3cfc8f3db` |

Raw and parsed hashes are equal because pandas wrote these caches with ISO timestamps and
`repr`-style floats, and neither day has a pandas NA string, so every value prints the same before
and after parsing. (An earlier version hashed only `mmsi,ts`; it matched too.)

Rows per stage:

| Stage | Block 0 (7 days) | 84 days |
|---|---|---|
| rows in = standardized (0 rejected) | 3,313,891 | 31,919,529 |
| Class A | 3,279,793 | 31,311,003 |
| (mmsi, ts) duplicate rows (groups disagreeing on nav/sog) | 860,606 (1,600) | 7,766,403 (14,778) |
| after minute thinning and dedupe | 524,638 | 5,171,358 |
| (block, mmsi) groups / never enter / start inside | 155 / 7 / 84 | 1,619 / 106 / 966 |
| arrivals / large | 64 / 35 | 547 / 335 |
| stops (anchored) / moored elsewhere | 2 (2) / 0 | 52 (24) / 0 |
| ETA rows / with ETA / kept <= 7 d | 35 / 35 / 34 | 335 / 335 / 317 |

Runtimes (driver 4g, `local[*]`, OrbStack): block 0 ingest 47 s, arrivals 42 s, compare 13 s;
84 days ingest 463 s (standardized 256 s, quarantine 152 s), arrivals 264 s (dedupe count 75 s,
thinning 82 s, groups 15 s, arrivals 33 s, stops 24 s, ETA 15 s, write 16 s), compare 17 s,
order check 19 s.

Comparison, block 0 and all 84 days:

| | Block 0 | 84 days |
|---|---|---|
| arrivals matched / only Spark / only October | 64 / 0 / 0 | 547 / 0 / 0 |
| stops matched / only Spark / only October | 2 / 0 / 0 | 52 / 0 / 0 |
| ETA matched / only Spark / only October | 35 / 0 / 0 | 335 / 0 / 0 |
| field differences (any field, incl. cut_at_start, stop lat/lon, stop's arrival) | 0 | 0 |

84-day summary, identical on both sides and to `year_comparison.json`: 547 arrivals, 335 large,
24 large anchor waits (7.16 %), median wait 12.57 h, max 46.68 h, 12 waits of 12 h or more,
2 censored at the block start, 0 moored stops elsewhere. The October "104 waits a year" is a
naive extrapolation, 24 / 84 days x 365, under the R1 definitions (first entries, ships that
start the block outside the port, anchor stops of 30+ min), not a measured total. ETA reported at
entry: 317 kept (18 dropped by the 7-day rule), 75.7 % within 2 h, median absolute error 1.02 h,
max 138.2 h.

Nordby: 2 arrivals enter the zone inside the NORDBY_ZONE rectangle, both the same 8 m SAR boat
(FRB-06), 0 large. The Fanoe ferries never count as arrivals: both their ends lie inside the
zone, so every block starts with them inside and they are skipped.

Negative control: the same comparison against a copy of the October CSVs with four planted
changes (one wait +1 min, one arrival +1 s, one arrival deleted, one stop share -0.01) reports
545 / 2 / 1 for arrivals, 1 difference in wait_min and anchor_wait_min, and 1 in anchored_share.
So zero mismatches is a result, not a blind comparison. The clean run exits 0
(`RESULT: PASS`); the negative control exits 1 with 8 failed checks listed.

Tie definitions: after Class A filtering, a (mmsi, ts) group with more than one row is a tie
group (7,291,630 groups, 7,766,403 surplus rows). It is "conflicting" when its rows differ in nav
or sog: 14,778 groups when a missing value counts as a value of its own (the Spark count above),
14,685 when missing values are ignored (a pandas `nunique` count, from the planning profile).
Either way the first row of the minute in file order wins, on both sides.

Learned on the way:
- The compose stack can deadlock: with blocks already in the namenode volume (from S2), the
  namenode stays in safe mode until a datanode reports, and the datanode waits for the namenode to
  be healthy (that is, out of safe mode). Worked around by starting the datanode with
  `--no-deps`; the healthcheck or the dependency needs a fix (for the owners, not changed here)
- An empty quarantine (no rejects) has no Parquet files, so its schema cannot be inferred: the
  lake tables are read with their explicit schema
- Spark does not allow `count(DISTINCT …)` over a window; the ETA tie check compares min and max
- OrbStack also starts the S3a Kubernetes rehearsal pods; with them running, the Spark driver got
  4g

### Decisions to review in R2

These are choices in the October method, ported as they are. Some may well stay.

- **Nordby overlap.** NORDBY_ZONE (lon 8.395-8.418) mostly lies inside the harbour zone
  (lon from 8.405), and only "not in the zone" is ignored, so the ignore rule covers just the
  strip lon 8.395-8.405. A ship docking at Nordby counts as entering Esbjerg. In the 84 days:
  2 arrivals, both an 8 m SAR boat, 0 large
- **Class A filter.** Class B and other transponders are dropped before anything else. Not in the
  written method description
- **Blocks from ts dates**, not from the file a row came from. On this data no row has a date
  other than its file's, so it makes no difference here
- **"anchor" substring** rather than exactly "At anchor". On this data only "At anchor" contains it
- **Chunk-dependent thinning** (per 500,000 lines). No effect on these files, but a larger file
  would thin differently depending on where the chunk boundary falls
- **Ship type over all loaded days**: a run on fewer days can give a ship another type, so
  results depend on the sample, not only on the block
- **ETA ties**: undefined in pandas for equal ts; did not occur here
- **Ships that start a block inside the port**: 966 of 1,619 (block, mmsi) groups are skipped
  because the ship starts the block inside the port; together with first-entry-only, "547
  arrivals" means first entries by ships that started the block outside the port
- **Junk speeds** are not filtered (a SAR helicopter reports 102 kn); they never make a row
  stationary, so they do not change stops

## R2: method fixes, planned

- **Repeat visits**: only the first entry per (block, mmsi) is counted, so a ship that calls twice
  in a week, or leaves and waits again, is one arrival
- **ETA at a fixed lead time**: compare the ETA a ship reported a fixed time before entry (for
  example 24 h and 6 h), instead of the last one before entry, which is close to the answer
- The decisions above, each with the change in numbers before and after

## D1: DMA days into the lake (#10)

Two steps, each a one-off Job in `infra/k8s/spark/school/one-off/`:

1. `scripts/load_dma.py` (Job `load-dma`): each DMA zip, read with HTTP range requests, is streamed
   unzipped to WebHDFS as `/lake/landing/dma/day=<day>/aisdk-<day>.csv`. A hidden temp file plus a
   rename with overwrite replaces a day in one step. Nothing touches the pod's disk (read-only root
   filesystem). A day DMA has not published yet (404) is skipped, exit code 3.
2. `spark/jobs/clean_dma.py` (Job `clean-dma`, cluster mode): the Esbjerg rows of each day go to
   `/lake/standardized/ais_positions/source=dma/day=<day>/` in the schema of the October files
   (`STANDARDIZED_SCHEMA`). Bad rows go to `/lake/quarantine/ais_positions/source=dma/` with
   their reason, and a load log per day to `/lake/standardized/_loads/source=dma/day=<day>/`.
   The rules are in `spark/common/dma.py`. Vessel rows (Class A or B) inside the
   `config/ports.yaml` box are picked before anything shuffles. Rows without a possible position
   are kept only for ships seen in the box that day (a broadcast semi-join).

Quarantine reasons: `mmsi_invalid` (not 9 digits starting 2 to 7), `position_impossible`
(missing, unparsable, or lat 91 / lon 181), `ts_unparsable`, `duplicate`. `line_no` is the row's
position among the day's port rows in file order, as in the portarea files.

**What a duplicate is:** the full row. Two rows of the same day are duplicates only when all 26
DMA columns are equal as text: `# Timestamp`, `Type of mobile`, `MMSI`, `Latitude`, `Longitude`,
`Navigational status`, `ROT`, `SOG`, `COG`, `Heading`, `IMO`, `Callsign`, `Name`, `Ship type`,
`Cargo type`, `Width`, `Length`, `Type of position fixing device`, `Draught`, `Destination`,
`ETA`, `Data source type`, `A`, `B`, `C`, `D`. The first one in file order is kept, the later ones
go to quarantine. These are the same message received more than once.

Two different positions (or any other difference, even only in `Data source type`) from the same
ship in the same second are **not** duplicates: both are kept in `standardized`. The arrivals
analysis then keeps the first of them in file order (`class_a_thinned`, the same rule as for the
October files) and reports them as ties. On 2026-10-01, 1,114 (MMSI, second) pairs of the kept
rows have more than one row. For Class A that is 1,105 pairs with 1,115 extra rows; 205 of them
disagree on the navigational status or SOG.

### 2026-10-01 on the local OrbStack rehearsal (27 Nov)

| | |
|---|---|
| Landing CSV | 3,840,321,158 bytes, 21,175,402 rows, loaded in 23.0 s (1 HEAD + 4 range GETs) |
| Rows in / vessel rows / in the box | 21,175,402 / 19,482,462 / 378,643 |
| Port rows (box + 45 rows without position of ships in the box) | 378,688 |
| Kept | 268,160 (6,208 Class B), 87 MMSI |
| Quarantined | 110,528: duplicate 101,163, mmsi_invalid 24,234, position_impossible 45, ts_unparsable 0 (a row can have two reasons) |
| Invalid MMSI | `2190045` (7 digits, 23,314 rows) and `111219516` (a SAR aircraft number sent as Class A, 920 rows) |
| Parquet | 7.4 MB standardized, 3.1 MB quarantine |
| Shuffle | 46.4 MB, 380,875 records: only the port rows (the 3.97 GB read is narrow) |
| Cleaning runtime | 58.4 s with 2 executors, 59.2 s with 1 (1 GiB, 2 cores each; one local datanode) |
| Arrivals analysis, unchanged, on this day | 13 arrivals (7 large, all 7 with an ETA), 43.8 s |

Ships with no ship type on any kept row: 7 of 87; with no name: 2. The analysis takes both
from other rows of the same ship, as for the October files.

The Esbjerg box (`config/ports.yaml`) covers the anchorage. Over 1, 8, 15, 22 and 29 October, the
ships lying still or at anchor off Esbjerg were at 55.40 to 55.45 N, 8.10 to 8.20 E, inside the
box. The still ships outside it were Rømø harbour, Hvide Sande, the Horns Rev 1 wind farm
transponder and anchoring off Rømø / Lister Dyb.

## R3: fresh full DMA days, planned

Download full DMA day files and cut them to the Esbjerg box in Spark (replacing the pandas
`scan_dma_ports` step that made the portarea caches), then run R1a-R1c on them and compare with
the cache-based run.
