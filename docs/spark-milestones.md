# Spark milestones

Owner: Vedat. Branch: `Spark-Vedat`. Nothing goes to `main` until the PR at the end.

Each milestone: plan, implement, show real output, review, commit.

| Milestone | What | Status |
|---|---|---|
| S1a | Fix the Spark image crash (Python 3.10) | Done, commit `43f365b` |
| S1b | Spark running as pods on local Kubernetes (OrbStack) | Done, commit `ce76738` |
| S2 | Kafka-to-HDFS streaming job | Done |
| S3 | Run on the school cluster | Blocked |
| PR | One pull request to `main` | After S2 or S3 |

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
  (its HDFS uses `dfs.replication` 1): add it to the school Spark configuration in S3

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

Scope: get the Spark image onto the school cluster and run the S1b and S2 jobs in namespace `bd-gr-05`.

Blocked on (ask Gerardas):
- Where do we push images? The cluster has no registry.
- Does the cluster have internet access?
- Spark 4.2 jobs need either `networkpolicies` in the `spark` Role or the NetworkPolicy step skipped in the school overlay.
- Spark jobs need `spark.hadoop.dfs.replication=1` there too (see S2).

Done when: the S1b and S2 checks pass on the school cluster.
