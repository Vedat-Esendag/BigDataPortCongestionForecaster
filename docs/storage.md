# Redis and Cassandra: how they work in this project

This explains what the two serving stores hold, where their data comes from, why they are built
the way they are, and how to run and inspect them. For the whole data flow see
[architecture.md](architecture.md); for the connectors that fill them see
[connectors.md](connectors.md).

## The short version

The project has four places that hold data. Each one has its own job:

| Store | Role | Holds | If it is lost |
|---|---|---|---|
| **HDFS** | The main store (the data lake) | Raw data, cleaned data, results, models, checkpoints. Everything, kept indefinitely | Real data loss |
| **Kafka** | The conveyor belt between stages | Recent messages only (days) | The pipeline stops; HDFS still has the raw data |
| **Cassandra** | Serving store: what happened | Every prediction and outlook over time, analytics results | Rebuildable from HDFS |
| **Redis** | Serving store: what is true now | The latest prediction per ship and outlook per port | Refills from the next updates |

Redis and Cassandra sit at the **end** of the pipeline. They never see raw AIS messages. They
only receive **finished results** that Spark computed and wrote to Kafka, copied in by Kafka
Connect sinks. No other component writes to them. The web API only reads from them.

```
raw AIS ─► Kafka ais.position ─► Spark live job (clean, port events, features, predict, outlook)
                                        │  a micro-batch every 10 seconds
                                        ▼
                    Kafka: ship.prediction (key = MMSI), port.outlook (key = port)
                        │                         │                       │
                  Redis sink               Cassandra sink           HDFS sink
                  overwrites               keeps every version      archive as Parquet
                        │                         │
                        └──────────► API ◄────────┘ ──► dashboard
```

Neither store does analytics itself. The thinking happens in Spark. Redis and Cassandra keep
Spark's answers in a shape the dashboard can read in milliseconds.

**Historical replays never reach them.** The `ais.history.*` topics (old DMA data replayed for
training) go only to HDFS. The Redis and Cassandra sinks list their topics by name, with no
wildcards, so a replay of 2024 data can never appear on the live dashboard as if those ships
were arriving today.

---

## Redis: the latest state

### What it holds

One JSON document per ship and one per port. Each new result **overwrites** the previous one,
so Redis always answers "what is the situation right now", never "what was it an hour ago".

| Key | Value | Written from topic |
|---|---|---|
| `ship.prediction:<mmsi>` | The ship's latest prediction | `ship.prediction` |
| `port.outlook:<port>` | The port's latest outlook | `port.outlook` |

A ship document, following [ship.prediction.avsc](../schemas/avro/ship.prediction.avsc):

```json
{ "mmsi": 219000123, "port": "esbjerg",
  "broadcast_eta": 1794841200000, "predicted_eta": 1794845400000,
  "eta_lower": 1794843900000, "eta_upper": 1794847200000,
  "model_version": "v3", "reason_codes": ["high_wind", "slow_approach"],
  "lat": 55.41, "lon": 8.12,
  "event_time": 1794834130000, "processing_time": 1794834138000, "day": "2026-11-16" }
```

That single document holds everything the map and the ship table need: position, both ETAs,
the uncertainty window, the explanation codes, and two timestamps. `processing_time` is what lets
the dashboard show how old each number is ("updated 40 s ago").

Because writes overwrite, Redis stays the same size however long the system runs: roughly one key
per ship in the area, plus one per port.

### Where it comes from

The Redis sink connector ([redis-sink.json](../connect/redis-sink.json)) reads `ship.prediction`
and `port.outlook`. It builds each key from the topic name and the Kafka record key, and stores
the record as JSON (`redis.type: JSON`). Redis 8 has the JSON type built in, so no extra module
is needed.

### How live updates reach the dashboard

The planners expect the screen to update by itself. The Redis sink can write keys, but it cannot
send pub/sub messages (see [connectors.md](connectors.md)). So Redis announces every change
itself:

1. Redis runs with `notify-keyspace-events KA` ([redis.conf](../infra/k8s/base/redis/redis.conf)).
   After every write it publishes the name of the changed key on a channel, for example
   `__keyspace@0__:ship.prediction:219000123`, with the message `json.set`.
2. Every API pod subscribes to the pattern `__keyspace@0__:ship.prediction:*` (and the same for
   `port.outlook:*`).
3. When a message arrives, the API reads that key (`JSON.GET`) and pushes it to its connected
   dashboards over WebSocket.

Pub/sub is fire-and-forget: an API pod that is briefly disconnected misses those messages. So
a dashboard reloads the full state whenever its WebSocket reconnects.

This is also why the API can run as several identical pods: no pod keeps state of its own. Each
listens to Redis and serves whichever dashboards happen to be connected to it.

### Why Redis, and why built this way

- **Speed.** Everything is in memory, so a lookup takes well under a millisecond. This is the
  hot path for the "under 60 seconds from AIS message to screen" target.
- **Degraded mode.** If Spark or the AIS feed stops, Redis still holds the last known state, and
  the timestamps show it is getting old. The dashboard shows a stale picture, clearly labeled as
  stale, instead of going blank.
- **Persistence (AOF).** Redis writes every change to an append-only file on its volume and
  replays it on start. Without it, a restart would show an empty map until every ship sent a new
  position, which can take many minutes for a ship at anchor. At most one second of updates is
  lost (`appendfsync everysec`).
- **No eviction.** If Redis ever fills its 512 MB cap, it rejects writes (the sink logs an error)
  rather than quietly dropping ships from the map.

### Known gaps (to do)

1. **The outlook key holds only one slot.** An outlook has many time slots (the next 24 to 48
   hours), but they all share the key `port.outlook:<port>`, so each slot overwrites the one
   before. Fix: put the slot in the key (`port.outlook:<port>:<slot_start>`), or have Spark write
   one document per port with all slots inside. Either way the Kafka key written by
   `spark/streaming/live_job.py` changes, and that also affects the ksqlDB alerts.
2. **Ships never leave Redis.** A ship that berthed or sailed away keeps its key. The API already
   hides it from the ship list (see "How the API reads them" below), but the key itself stays
   and Redis slowly grows. Fix: an expiry on each key, if the sink connector can set one.
3. **Listing all ships of a port needs a scan.** The API runs `SCAN` over every ship key. That is
   fine for one port with a few hundred ships; a per-port set of MMSIs would scale better.

---

## Cassandra: the history

### What it holds

**Every** prediction and outlook the live job produced, plus the results of the batch jobs. Where
Redis overwrites, Cassandra keeps each version. If ship 219000123 received 100 predictions
today, all 100 are rows in Cassandra.

| Table | From topic | Produced by | One row per |
|---|---|---|---|
| `ship_prediction_by_port_day` | `ship.prediction` | Spark live job | prediction |
| `port_outlook_by_port_day` | `port.outlook` | Spark live job | outlook slot, per computation |
| `weather_delay_by_port` | `analytics.weather_delay` | Spark batch | port, weather variable, bucket |
| `eval_metrics_by_port_day` | `eval.metrics` | Spark batch | day, model version, horizon, metric |

The tables are defined in [cassandra/schema.cql](../cassandra/schema.cql). The keyspace is
`portcongestion`.

### Where it comes from

The DataStax Cassandra sink ([cassandra-sink.json](../connect/cassandra-sink.json)) reads the four
topics above and maps each Avro field to the column of the same name. Batch results take the same
route as live results (Spark to Kafka to sink), so nothing in the project writes to Cassandra
directly.

### How the tables are designed

Cassandra is fast only when a query matches how the data is laid out on disk. So the tables are
designed **from the dashboard's questions**, not from the shape of the data. Take the
predictions table:

```sql
PRIMARY KEY ((port, day), event_time, mmsi)
WITH CLUSTERING ORDER BY (event_time DESC, mmsi ASC)
```

- **`(port, day)` is the partition key.** Every prediction for Esbjerg on one day is stored
  together, on one node. "Show today's predictions" reads exactly one partition. The day is part
  of the key so that no partition grows forever, which Cassandra handles badly.
- **`event_time DESC` is the clustering order.** Inside a partition rows are kept newest first,
  so "the latest 200" is a straight read with no sorting.

A typical query from the API:

```sql
SELECT * FROM ship_prediction_by_port_day
WHERE port = 'esbjerg' AND day = '2026-11-16'
LIMIT 200;
```

### Why repeated writes don't create duplicates

In Cassandra, writing a row whose primary key already exists **replaces** that row. Kafka Connect
delivers messages *at least once*, so after a crash or a restart it may send some records again.
Those land on the same `(port, day, event_time, mmsi)` and simply replace themselves. The result
is no duplicates after a restart, which is one of the project's reliability requirements, and it
needs no extra code.

The analytics table uses the same rule on purpose: a new analytics run overwrites the previous
result for the same port, variable and bucket.

### Why Cassandra and not HDFS or Redis for history

- **HDFS** is built for Spark jobs that scan large Parquet files, which takes seconds to minutes.
  It is a poor fit for "give me 200 rows for this port and day" within a web request.
- **Redis** keeps everything in memory and has no proper time-range queries. Keeping months of
  history there would be expensive and awkward.
- **Cassandra** keeps long history on disk cheaply and answers in milliseconds, as long as the
  query names the partition key.

### What the dashboard uses it for

- The **timeline**: how a ship's predicted ETA changed during the day.
- **Outlook history**: what the outlook said six hours ago, next to what actually happened.
- The **analysis page** (weather against delay), from `weather_delay_by_port`.
- **Evaluation** (F12): our ETA against the broadcast ETA and the no-weather baseline, from
  `eval_metrics_by_port_day`.

### Known gaps (to do)

1. **No table for one ship's history.** All tables are partitioned by port and day, so "the ETA
   history of ship X" means reading the whole day and filtering. If clicking a ship should show
   its history, add a table such as `ship_prediction_by_mmsi_day` with
   `PRIMARY KEY ((mmsi, day), event_time)`. The sink can write one topic into two tables.
2. **Volume.** The live job writes a row on every update for every ship. A busy day still fits
   comfortably in one partition, but consider a `default_time_to_live` on the tables, or only
   writing when the ETA actually changes.

---

## How the API reads them

The API (`services/api`) is the only reader. It never writes to either store.

| Endpoint | Store | What it does | Code |
|---|---|---|---|
| `GET /ships?port=` | Redis | `SCAN` for `ship.prediction:*`, one `JSON.MGET`, keep this port, hide departed ships, sort by predicted arrival | `clients/redis_client.py` |
| `GET /ships/{mmsi}` | Redis | `JSON.GET ship.prediction:<mmsi>`, 404 if missing | same |
| `GET /outlook?port=` | Redis | Every `port.outlook:<port>` key (with or without a slot suffix), earliest slot first | same |
| `GET /history/ships?port=&day=&limit=` | Cassandra | One partition of `ship_prediction_by_port_day`, newest first | `clients/cassandra_client.py` |
| `GET /history/outlook?port=&day=&limit=` | Cassandra | One partition of `port_outlook_by_port_day`, newest first | same |
| `WS /ws` | Redis keyspace events | Pushes `ship` and `outlook` messages as Redis announces changes | `live.py` |
| `GET /ready` | Redis | 200 if Redis answers, 503 if not. Also reports Cassandra, for information | `routes/health.py` |

**Departed ships.** A ship whose last prediction is more than 6 hours (`SHIP_MAX_AGE_MINUTES`)
older than the newest prediction of any ship is left off `/ships`. The age is measured against
the newest ship, not against the clock. If the whole pipeline stops, all ships stop updating
together and stay visible with their last known state, which is the degraded mode the project
asks for.

**When a store is down** the endpoints that need it answer **503** with
`{"detail": "...", "store": "redis" | "cassandra"}`, never a 500. A missing Cassandra only
breaks the history endpoints. The live view and the push keep working, and `/ready` stays 200,
so Kubernetes keeps the pod in service.

**Broken records** (a field missing, a wrong type) are skipped one by one, logged, and counted
in the metric `api_invalid_documents_total{store=...}`. One bad document doesn't empty the
ship list.

**Timestamps** go out as ISO 8601 in UTC (`2026-11-16T16:00:00Z`). Redis documents hold epoch
milliseconds and Cassandra returns naive datetimes; the API marks both as UTC.

**The live feed** is one background task per API pod (`LiveFeed` in `live.py`). It subscribes to
`__keyspace@0__:ship.prediction:*` and `__keyspace@0__:port.outlook:*`, reads each changed key,
and sends it to that pod's WebSocket clients. A client can send
`{"type": "subscribe", "port": "dkebj"}` to get only one port; until then it gets every port.
If Redis goes away, the feed reconnects by itself (1 s, 2 s, 4 s ... up to 30 s). The metric
`api_live_feed_connected` is 1 while it listens. A browser that is too slow to take a message
within 5 seconds is disconnected; the dashboard reconnects and reloads.

**Testing.** `tests/test_smoke.py` and `tests/test_live.py` run without any store.
`tests/test_integration.py` runs against a real Redis and Cassandra and skips itself when they
aren't running. It covers the whole live path: a write to Redis arrives on a WebSocket.

---

## Running them on Kubernetes

Manifests: [infra/k8s/base/redis/](../infra/k8s/base/redis/) and
[infra/k8s/base/cassandra/](../infra/k8s/base/cassandra/).

| | Redis | Cassandra |
|---|---|---|
| Workload | Deployment, 1 replica, `Recreate` | StatefulSet, 1 node |
| Address in the namespace | `redis:6379` | `cassandra:9042` |
| Volume | claim `data-redis` (1 Gi) at `/data` | claim `data-cassandra-0` (10 Gi) at `/var/lib/cassandra` |
| Settings | [redis.conf](../infra/k8s/base/redis/redis.conf) (also used by docker compose) | environment variables in the StatefulSet |
| Setup | none | Job `cassandra-init` runs `schema.cql` |

### Surviving restarts

- **Redis** replays its append-only file on start, so it comes back with the state it had
  about one second before it stopped.
- **Cassandra** runs `nodetool drain` just before its pod stops, which writes everything still in
  memory to disk. It would also recover from its commit log without that, only slower.
- Both volumes use the `hostpath-custom` storage class on the school cluster (set in
  `overlays/school`). That disk belongs to the cluster's single node: the data survives pod
  restarts and redeploys, but not the loss of the node itself.

### The keyspace Job

`cassandra-init` waits until Cassandra answers, runs `schema.cql`, and prints the resulting
tables. It runs on every deploy. That is safe because every statement says `IF NOT EXISTS`.
Kubernetes removes the finished Job after five minutes, because a Job can't be changed once
created, and a leftover one would block the next deploy after a schema change. This is why
`make k8s-diff` always lists it as new.

`schema.cql` is the only copy of the schema. Docker compose mounts it, and Kubernetes builds a
ConfigMap from it through [cassandra/kustomization.yaml](../cassandra/kustomization.yaml).

To **change** an existing table, add an `ALTER TABLE` statement to `schema.cql`. Editing a
`CREATE TABLE ... IF NOT EXISTS` does nothing to a table that already exists.

### What deletes data

- Restarting or deleting a **pod**: no data lost.
- `kubectl delete -k infra/k8s/overlays/school`: **deletes the Redis claim** (`data-redis` is
  listed in the manifests), so Redis starts empty next time. Cassandra's claim is kept, because
  Kubernetes never deletes claims created by a StatefulSet.
- `kubectl delete pvc data-cassandra-0`: deletes all Cassandra history. Only do this on purpose,
  and tell the group first.

### Looking inside

```bash
source infra/k8s/cluster/env.sh

# Redis
kubectl exec deploy/redis -- redis-cli info keyspace                       # how many keys
kubectl exec deploy/redis -- redis-cli --scan --pattern 'ship.prediction:*' | head
kubectl exec deploy/redis -- redis-cli JSON.GET ship.prediction:219000123
kubectl exec -it deploy/redis -- redis-cli psubscribe '__keyspace@0__:*'   # watch writes live (Ctrl+C)

# Cassandra
kubectl logs job/cassandra-init                                            # did the schema apply?
kubectl exec cassandra-0 -- cqlsh -e 'DESCRIBE KEYSPACE portcongestion'
kubectl exec cassandra-0 -- cqlsh -e "SELECT * FROM portcongestion.ship_prediction_by_port_day WHERE port='esbjerg' AND day='2026-11-16' LIMIT 10"
```

`make k8s-check` also checks that Redis has persistence and keyspace events turned on, and
that the keyspace exists.

### When something is wrong

| Symptom | Likely cause |
|---|---|
| Redis or Cassandra pod logs `Permission denied` on its data folder | The volume isn't writable for the non-root user. `fsGroup: 999` normally fixes this, but some storage types ignore it. |
| Cassandra pod stuck `Pending` | Its volume claim can't be bound. `kubectl describe pvc data-cassandra-0` shows why. |
| `keyspace portcongestion exists` fails in `make k8s-check` | The Job failed or hasn't finished. `kubectl logs job/cassandra-init`; if the Job is already gone, deploy again. |
| `kubectl apply` fails with "field is immutable" on `job/cassandra-init` | The previous Job is less than five minutes old. Wait, or `kubectl delete job cassandra-init`, then deploy again. |
| Dashboard doesn't update live, but a page refresh shows new data | Keyspace events are off. `kubectl exec deploy/redis -- redis-cli config get notify-keyspace-events` should include `K`. |
| Redis sink logs `OOM command not allowed` | Redis reached its 512 MB cap. Look for keys that never expire (see the known gaps above). |
