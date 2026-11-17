# Spark jobs

| Folder | What |
|---|---|
| `common/` | shared code: Avro on Kafka (`avro_io.py`), config and lake paths, cleaning, features |
| `streaming/` | streaming jobs: `live_job.py` (predictions), `kafka_to_lake.py` (raw copy to HDFS) |
| `batch/`, `training/` | batch analytics, evaluation, model training |
| `jobs/` | small check jobs (`smoke_count.py`, `lake_counts.py`) |
| `tests/` | `make test` runs these; fixtures in `tests/fixtures/` |

The image (`spark/Dockerfile`) has the Kafka and Avro connectors baked in, so no job needs
`--packages` or internet access at run time.

## Record live AIS in the data lake (kafka_to_lake)

`streaming/kafka_to_lake.py` copies one Kafka topic, unfiltered, into the raw zone of the lake as
Parquet: `/lake/raw/<topic with dots as underscores>/day=yyyy-MM-dd/` (UTC day of `event_time`).
Each row keeps its Kafka partition, offset and timestamp. The checkpoint is
`/checkpoints/kafka_to_lake/<topic>`, so a restart continues where it stopped without writing
anything twice. Run one job per topic.

All commands from the repo root, local docker-compose stack only.

1. Start only what the job needs (HDFS runs emulated on Apple Silicon and takes a minute or two):

   ```bash
   docker compose -f infra/docker-compose.yml --profile core --profile storage --profile apps up -d --build kafka schema-registry namenode datanode hdfs-init spark
   ```

2. Create the topics and register the schemas:

   ```bash
   scripts/create_topics.sh
   ```

   ```bash
   make schemas
   ```

3. Start the job, one terminal per topic (it runs until you stop it with Ctrl+C):

   ```bash
   make spark-lake TOPIC=ais.position
   ```

   ```bash
   make spark-lake TOPIC=ais.static
   ```

   Options go in `LAKE_ARGS`, for example `make spark-lake LAKE_ARGS="--trigger available-now"`:
   `--trigger "30 seconds"` (default `1 minute`) or `--trigger available-now` to process what is
   there and stop; `--checkpoint <hdfs uri>`; `--time-column` for a topic whose timestamp field
   is not `event_time`. The make target also sets `spark.hadoop.dfs.replication=1`, because the
   compose HDFS has a single datanode.

4. Send test data: the 20 rows of `tests/fixtures/ais_sample.csv` to `ais.position` and the 6
   rows of `tests/fixtures/ais_static_sample.csv` to `ais.static`:

   ```bash
   uv run --no-project --with 'confluent-kafka[avro,schemaregistry]' python scripts/send_ais_fixtures.py
   ```

5. After the next trigger, look at the files:

   ```bash
   docker compose -f infra/docker-compose.yml --profile storage exec namenode hdfs dfs -ls -R /lake/raw
   ```

   Read them with Spark from a `pyspark` shell in the spark container
   (`docker compose -f infra/docker-compose.yml --profile apps exec spark /opt/spark/bin/pyspark`):

   ```python
   spark.read.parquet("hdfs://namenode:8020/lake/raw/ais_position").groupBy("day").count().show()
   ```

To start over, stop the jobs and delete both the output and its checkpoint (`hdfs dfs -rm -r
/lake/raw/ais_position /checkpoints/kafka_to_lake/ais.position`). Deleting only one of them
makes the job skip or rewrite data.
