# Kafka Connect plugins

One plugin per job. Choice criteria: (a) works with Kafka 4.x and the current Confluent Platform,
(b) free to use in a university project, (c) still maintained, meaning a release in roughly the
last 12 months. Facts were looked up on 2026-10-27 (GitHub, Maven Central, Confluent Hub).

| Job | Plugin | Version | License | Last release | Source |
|-----|--------|---------|---------|--------------|--------|
| DMA CSV to Kafka | Kafka Connect FilePulse | 2.18.0 | Apache-2.0 | 2026-06-26 | [GitHub](https://github.com/streamthoughts/kafka-connect-file-pulse) |
| HTTP source (Open-Meteo, DMI) | Apache Camel Kafka Connector, `camel-http-source` | 4.22.0 | Apache-2.0 | 2026-09-24 | [GitHub](https://github.com/apache/camel-kafka-connector), Maven Central |
| Parquet to HDFS | Confluent HDFS 2 Sink | 10.2.17 | Confluent Community License | 2025-07-18 | [GitHub](https://github.com/confluentinc/kafka-connect-hdfs), Confluent Hub |
| Cassandra sink | DataStax Apache Kafka Connector | 1.7.6 | Apache-2.0 (repository) | 2026-06-25 | [GitHub](https://github.com/datastax/kafka-sink) |
| Redis sink | Redis Kafka Connector | 1.1.0 | Apache-2.0 (per GitHub) | 2025-09-03 | [GitHub](https://github.com/redis-field-engineering/redis-kafka-connect) |
| Alert webhook sink | Lenses Stream Reactor, HTTP sink | 12.1.2 | Apache-2.0 | 2026-09-14 | [GitHub](https://github.com/lensesio/stream-reactor) |

Versions are pinned in `versions.env` and installed by `connect/Dockerfile`. Converters (Avro
with Schema Registry) come with the Confluent Platform image.

## Flags

**No plugin in the table needs a commercial license.** Two commercial connectors exist for these
jobs and were deliberately not used: Confluent's *HTTP Source* and Confluent's *HDFS 3 Sink*
(Confluent Software Evaluation License). The free *HDFS 2 Sink* in the table writes Parquet and
works with a Hadoop 3 cluster. It is under the Confluent Community License: free to use, but not
to offer as a competing hosted service, which does not matter for a course project.

**The Redis sink cannot publish to pub/sub.** The Redis Kafka Connector writes STRING, JSON, HASH,
STREAM, LIST, SET, ZSET and TIMESERIES, but has no PUBLISH mode. The one open-source sink that did
(Lenses Stream Reactor `STOREAS PubSub`) was retired in Stream Reactor 11.0.0. What we do instead:
the sink keeps the latest state as one JSON document per key (`ship.prediction:<mmsi>`,
`port.outlook:<port>`) and Redis is started with `--notify-keyspace-events KA`, so every write is
announced on the pub/sub channel `__keyspace@0__:<key>`. The API subscribes to those channels
(pattern subscribe), reads the changed key, and pushes it over the WebSocket. Trade-offs: the
event carries only the key, so there is one extra read per update, and pub/sub messages are lost
if the API is disconnected (it reloads the full state on reconnect). Apache Camel's Redis sink
can PUBLISH and is the fallback if this proves too chatty.

## Why these and not the alternatives

| Job | Rejected | Reason |
|-----|----------|--------|
| DMA CSV | Spooldir (jcustenborder) | Apache-2.0 and works, but the last release is 2024-12 on Confluent Hub (2022-10 on GitHub), outside criterion (c) |
| HTTP source | castorm `kafka-connect-http` | last release 2021-03 |
| HTTP source | Confluent HTTP Source | commercial license |
| HDFS | Confluent HDFS 3 Sink | commercial license |
| Webhook | Aiven `http-connector-for-apache-kafka` | last release 2025-02 |
| Redis | Lenses Stream Reactor Redis sink | retired in 11.0.0 |
| Redis | `jcustenborder/kafka-connect-redis` | last release 2022-04 |
| Cassandra | Lenses Stream Reactor Cassandra sink | works too (12.1.2); DataStax's connector has the more complete, better documented field mapping |

Borderline on criterion (c): the HDFS sink (15 months) and the Redis sink (13.8 months) are a bit
past 12 months. Both repositories are active (commits in 2026) and nothing else free does the job.

## Not verified yet

None of these vendors states support for Kafka 4.x; they are built against the Connect API of
Kafka 3.x (FilePulse 3.9.0, DataStax 3.7.2). That API is backward compatible, so they are
expected to work, but nothing has run yet because Docker was not available when this was written.
The first real check is `scripts/deploy_connectors.sh --validate-only`, which asks Connect to
validate every config before anything is deployed. Known weak spots, in order of risk:

1. **Camel HTTP source output type.** The connector's documentation does not say whether the record
   body is a string or bytes. The configs assume a string; the `HoistField` transform wraps it as
   `payload`. If it is bytes, change `payload` to `bytes` in `weather.*.avsc` and cast it in Spark.
2. **Webhook sink property names** (`connect.http.request.content`, `.headers`) and its `{{value.x}}`
   templates were taken from the current docs but not run.
3. **FilePulse** CSV property names (`DelimitedRowFilter`, `skip.headers`) and the Avro schema it infers.
4. **DataStax mapping keys** for topics that contain dots (`topic.ship.prediction.portcongestion...`).
5. **Schema compatibility.** Connect-produced topics register their own schema on first write.
   `make schemas` registers ours first. The `SetSchemaMetadata` and field names are chosen so
   both are compatible, but this is only confirmed when the first record is written.
6. **DMI API.** The documentation page could not be fetched. The base URL, the `api-key` query
   parameter and the parameter id `wind_speed` in `connect/http-source-dmi.json` come from
   third-party client libraries. Check them against DMI's documentation; the station ids in
   `config/ports.yaml` are placeholders too.
7. **DMA download URL** in `scripts/download_dma.py` (the DMA host did not answer from the
   development sandbox). Override with `--url-template` if it is wrong.
8. **Platform licenses.** Schema Registry and ksqlDB are understood to be under the Confluent
   Community License (free to use, not to resell as a service); this was not re-checked.
   The Redis 8 license was not checked either (it is source-available, free for this use).
