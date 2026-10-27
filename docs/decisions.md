# Decisions

## 1. Versions chosen (looked up 2026-10-27)

Rule from the team: use the latest stable release of each component, not whatever happens to be
installed. Everything is pinned in [`versions.env`](../versions.env) (and the lock files of the
Python and Node projects). **None of this is matched to the course cluster.** When the cluster
versions are known, change `versions.env` and the pins that repeat it (`spark/pyproject.toml`
pins `pyspark`, `infra/k8s/manifests` repeat image tags); `spark/tests/test_versions.py` fails if
the pyspark pin and `versions.env` drift apart.

| Component | Version | Why this one |
|-----------|---------|--------------|
| Confluent Platform | 8.4.0 | Newest release that ships Kafka 4.x (KRaft only). The docs list releases up to 8.3 (Kafka 4.3); 8.4.0 images appeared on Docker Hub on 2026-10-26 and the 8.4.x branches exist in the source repositories. 8.4 is taken to ship Kafka 4.4 (CP 8.N has shipped Kafka 4.N so far, and Kafka 4.4.0 is the newest Apache release); confirm with `make up-core`. It is one day old, so if it misbehaves, set `CP_VERSION=8.3.2` |
| Spark | 4.2.0 | Latest on PyPI, Maven Central, Docker Hub and apache.org. There is no 4.2.x patch release yet. Scala 2.13, image `4.2.0-scala2.13-java21-python3-ubuntu` |
| Java | 21 (Temurin) | Per the team. Spark 4.2 also runs on 17, and the tests accept 17+, but install 21 (steps per OS in `docs/setup.md`) |
| Python | 3.12 | The team asked for 3.12+. Pinned to 3.12, not the newest (3.14), because PySpark requires the driver and the executors to run the same minor version and the Ubuntu-based Spark image ships 3.12 |
| Node | 24 (LTS "Krypton", 24.22.1) | Newest LTS. Node 26 is still "Current" on 2026-10-27, so it is not used; the local Node 26 builds the dashboard fine |
| Hadoop (HDFS) | 3.5.0 | The `stable` link on downloads.apache.org points to it. The official image is amd64 only, so it runs emulated on Apple Silicon |
| Cassandra | 5.0.9 | Latest GA. The Docker tag `latest` is 6.0 **alpha**, so the version is pinned explicitly |
| Redis | 8.8.4 | Latest. Redis 8 includes the JSON type used for latest state |
| Redpanda Console | v3.13.1 | Latest |
| Kafka lag exporter | danielqsj/kafka_exporter v1.11.0 | Latest. See decision 7 |
| Prometheus / Grafana | v3.15.1 / 13.2.5 | Latest |
| uv / ruff | 0.13.6 / 0.17.3 | Latest on PyPI (the local uv is older, 0.11.7; it works) |
| Connect plugins | see `docs/connectors.md` | Chosen by license, Kafka 4 compatibility and release recency |
| Strimzi / K8ssandra / kube-prometheus-stack | 1.3.0 / 1.33.0 / 92.14.0 | Latest releases. Strimzi 1.3.0 supports Kafka up to 4.4.0 |
| Dashboard | React 19.3.1, Vite 8.3.5, TypeScript 7.0.2, MapLibre GL JS 6.16.0, Recharts 3.10.1 | Latest from npm (`package-lock.json`) |
| Python libraries | locked by `uv.lock` | FastAPI 0.162, websockets 17, confluent-kafka 2.16, redis-py 8.1 and so on |

## 2. Choices made for the team

1. **Avro in Spark without schema evolution.** Confluent writes 5 bytes (magic byte and schema
   id) before every Avro payload, and Spark's `from_avro` does not understand them. The code
   strips the header and decodes with the schema in `schemas/avro/<topic>.avsc` (`spark/common/avro_io.py`),
   and writes the same way with the id looked up at start-up. **This ignores schema evolution:**
   the id in a message is never read, so if a producer starts writing a new schema version, the
   Spark jobs keep decoding with the old file until it is updated and the job restarted. That is
   acceptable while we own every producer. Alternatives: the ABRiS library, or reading the id and
   fetching that schema from the registry.
2. **DMA replay needs one more topic.** A DMA row can hold a position, static data or both, and a
   connector writes to one topic. FilePulse writes every row to `ais.history.raw` and
   `ksql/history_split.sql` splits it into `ais.history.position` and `ais.history.static`.
3. **Weather payloads stay raw JSON.** An HTTP source cannot unpack Open-Meteo's columnar hourly
   arrays into one record per hour, so `weather.*` carry `port`, `product`, `fetched_at` and the
   raw `payload`; Spark parses it (`common/features.py`, TODO(F6)).
4. **Batch results go through Kafka.** `spark-cassandra-connector` is not used (it supports only
   Spark 3.5). The batch jobs write to `analytics.weather_delay` and `eval.metrics` and the
   Cassandra sink stores them, the same way as the live results.
5. **Latest state in Redis is one JSON document per key**, written by the Redis sink, and live
   push uses Redis keyspace events because that sink cannot PUBLISH (`docs/connectors.md`).
   Redis 8 includes the JSON type, so no module is needed.
6. **Keys are plain strings** (MMSI or port id), values are Avro. No key schemas.
7. **Consumer lag: `kafka_exporter`, not KMinion.** Both are maintained and support Kafka 4
   (kafka_exporter reads KIP-848 consumer groups since v1.11.0). kafka_exporter has the simpler
   setup and a well-known Grafana metric set. Caveat: Spark Structured Streaming keeps offsets in
   its checkpoint and does not commit them to Kafka, so the exporter shows lag for the Connect
   sinks and ksqlDB but **not** for the Spark live job. Spark's own metrics (`/metrics/prometheus`
   on the driver, scraped as `spark-live`) must cover that; the Grafana dashboard says so.
8. **Map: MapLibre GL JS with OpenFreeMap** (`https://tiles.openfreemap.org/styles/liberty`).
   No API key and no sign-up. Data is from OpenStreetMap; the attribution the map shows must stay.
   The service documents limits only for offline tile downloads, so interactive use is fine for
   a demo; self-host tiles if this ever carries real traffic.
9. **Compose layout.** `infra/docker-compose.yml` only `include`s `infra/compose.services.yml`
   with `versions.env`. That keeps versions in one file and lets a bare
   `docker compose -f infra/docker-compose.yml config` work. Secrets reach only the containers
   that need them (`env_file` of the optional `.env`), and endpoints are set explicitly per
   service so host-side values in `.env` cannot leak into containers.
10. **No Bitnami charts** on Kubernetes (their free images were moved behind a paid catalog in
    2025, to the best of our knowledge). Strimzi for Kafka and Connect, K8ssandra for Cassandra,
    kube-prometheus-stack for monitoring, plain manifests for HDFS, Redis and our own services.
11. **FilePulse instead of spooldir** for the DMA CSV source, because spooldir has had no release
    since 2024-12 (see `docs/connectors.md`).
12. **`day` columns are text** (`yyyy-MM-dd`, UTC) so no component has to convert a date type.
13. **Redpanda Console, Prometheus and Grafana run without a healthcheck or with a simple one**;
    the Console image has no shell to run one with.
14. **The dashboard container serves static files with nginx** (`nginx:stable-alpine`, a base
    image only). Vite's own server is for development.
15. **Region and port: Denmark, Esbjerg only (`dkebj`).** Chosen by the team. This also settles
    the Danish-or-Norwegian question: DMA history and DMI wind observations both cover Denmark.
    The code is not tied to one port, so adding another is an entry in `config/ports.yaml` (and its
    DMI station id); the tests use invented ports to keep that case covered. Esbjerg's polygons and
    DMI station `06080` are still placeholders to replace with real values.

## 3. Open decisions

### Kappa replay via Connect versus loading DMA files straight into HDFS

- *Via Connect (what is scaffolded):* DMA files go through FilePulse into Kafka as `ais.history.*`
  topics, and the HDFS sink lands them in the lake. One path for live and historical data; Spark
  can replay history through the same code. Cost: DMA days are large (a CSV of 1 to 3 GB), a
  single connector task reads a file in order, and the history split adds a ksqlDB step.
- *Straight into HDFS:* `scripts/download_dma.py` plus `hdfs dfs -put` into `/lake/landing`, then a
  Spark batch job cleans them into `standardized`. Faster and simpler for bulk history; it gives up
  replaying history through Kafka.
- Both can coexist. Decide based on how much history training needs and whether replaying through
  Kafka is a course goal. The scaffold supports either.

### Spark on YARN versus Kubernetes

- The stack already has HDFS, which points at YARN on a Hadoop cluster. Kubernetes manifests
  (including Spark CronJobs) are provided because the course also offers a cluster.
- On YARN the CronJobs are replaced by a scheduler on the cluster, `--master yarn`, and Spark
  has to match the cluster's Hadoop version; Spark 4.2 needs Java 17 or 21 on every node.
- On Kubernetes the only requirement is a registry for the images in `spark/Dockerfile`.
- Needed from the course staff: the cluster's Hadoop, Spark, Kafka and Java versions.

## 4. Known stubs

Every stub carries a `TODO(Fx)` comment: `grep -rn "TODO(F" .` lists them. In short: cleaning
(F4), port events (F5), features (F6), training (F7), the real predictor and model loading (F8),
stateful outlook with waiting ships (F9), the API's Redis and Cassandra reads and the live push
(F10), the weather-delay analytics (F5 in the original spec, which also labels port events F5)
and the evaluation job (F12), and generating alert thresholds from `thresholds.yaml` (F13).
