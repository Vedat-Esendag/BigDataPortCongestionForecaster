# Spark milestones

Owner: Vedat. Branch: `Spark-Vedat`. Nothing goes to `main` until the PR at the end.

Each milestone: plan, implement, show real output, review, commit.

| Milestone | What | Status |
|---|---|---|
| S1a | Fix the Spark image crash (Python 3.10) | Done, commit `43f365b` |
| S1b | Spark running as pods on local Kubernetes (OrbStack) | Done |
| S2 | Kafka-to-HDFS streaming job | Next |
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

## S2: Kafka-to-HDFS job

Scope: a Structured Streaming job that reads `ais.position` from Kafka, decodes the Avro and writes Parquet to HDFS with a checkpoint. Built against docker-compose.

Decide first:
- Spark or Kafka Connect for the HDFS writes
- Kafka and Avro jars baked into the image, or downloaded at run time
- Which HDFS path to write to

Done when:
- A test message in Kafka ends up as a Parquet file on HDFS
- A restart writes no duplicates
- The Avro to Parquet format change is written down for the report

## S3: School cluster

Scope: get the Spark image onto the school cluster and run the S1b and S2 jobs in namespace `bd-gr-05`.

Blocked on (ask Gerardas):
- Where do we push images? The cluster has no registry.
- Does the cluster have internet access?
- Spark 4.2 jobs need either `networkpolicies` in the `spark` Role or the NetworkPolicy step skipped in the school overlay.

Done when: the S1b and S2 checks pass on the school cluster.
