# The whole stack on your laptop (local Kubernetes)

One command starts every service of the project in a local Kubernetes cluster, wires them
together and checks that it all answers:

```bash
make local-up
```

It runs the same manifests as the school cluster (`infra/k8s/base` and `infra/k8s/apps`), in the
same namespace (`bd-gr-05`), so what works here works there. The cluster is
[kind](https://kind.sigs.k8s.io/): Kubernetes running inside a Docker container. Nothing is
installed on your machine besides that container, and `make local-down` removes it again.

## What you get

| Service | What it does here | Address inside the cluster |
|---|---|---|
| `kafka` | One broker, data on a volume | `kafka:9092` |
| `schema-registry` | Avro schemas, all of `schemas/avro` registered | `http://schema-registry:8081` |
| `kafka-connect` | Our Connect image with every plugin, connectors deployed | `http://kafka-connect:8083` |
| `ksqldb` | The DMA history split and the outlook alerts running | `http://ksqldb:8088` |
| `redis` | Latest state, persistence and keyspace events on | `redis:6379` |
| `cassandra` | History, keyspace `portcongestion` with its tables | `cassandra:9042` |
| `hdfs-namenode`, `hdfs-datanode` | The data lake, `/lake`, `/models`, `/checkpoints` created | `hdfs://hdfs-namenode:8020` |
| `ais-producer` | Live AIS from aisstream.io into Kafka. Only with a key in `.env` | none (it only sends) |
| Spark | Service account and permissions for Spark jobs; jobs run by hand | |

**Wired together** means: the Kafka topics exist, the Avro schemas are registered, the ksqlDB
queries run, and the connectors are deployed. Not every connector works yet. This is what the
first real run showed (details and error messages in
[docs/connectors.md](../../../../docs/connectors.md#first-real-run-23-november-2026)):

| Connector | From | To | Status |
|---|---|---|---|
| `redis-sink` | `ship.prediction`, `port.outlook` | Redis | works, tested with a record |
| `dma-csv-source` | DMA CSV files in the Connect pod's `/data/dma/incoming` | `ais.history.raw` | starts; not yet tested with a real file |
| `cassandra-sink` | predictions, outlooks, analytics, evaluation | Cassandra | **reports RUNNING but writes nothing** (open) |
| `hdfs-parquet-sink` | every live and result topic | HDFS `/lake/raw` as Parquet | **fails**: Java 25 in the image (open) |
| `open-meteo-wind-dkebj`, `open-meteo-marine-dkebj` | Open-Meteo API | `weather.forecast` | **fails**: Camel payload type (open) |
| `dmi-wind-dkebj-<station>` | DMI API | `weather.observation` | works, no key needed (DMI dropped API keys in Dec 2025) |
| `webhook-sink` | `alerts` | your webhook | only with `ALERT_WEBHOOK_URL` in `.env` |

Because of the open ones, `make local-up` currently ends with *"The local stack is up, with
problems listed above"* and the smoke test lists the failing connectors. Everything else is up and
usable. The ksqlDB alert query was tested end to end: an outlook with 4 late ships produced the
alert `dkebj: late ships = 4` on the `alerts` topic.

Not here yet, because they have no Kubernetes manifests: the Spark live job, the API and the
dashboard. Run those from docker compose for now (see the main README).

## What you need

| Tool | Why | Check |
|---|---|---|
| Docker, with **at least 8 GB of memory** | kind runs the cluster as a container | `docker info` |
| kind, the version in `versions.env` (`KIND_VERSION`) | creates the cluster | `kind version` |
| kubectl | talks to the cluster | `kubectl version --client` |
| python3 with PyYAML, or uv | the setup scripts | `python3 -c 'import yaml'` |
| git, make | the commit tag of our images, the `make` targets | |

Install kind (put the version from `versions.env` in place of `v0.33.0`):

```bash
# Linux (x86_64)
curl -fsSLo ~/.local/bin/kind https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64
chmod +x ~/.local/bin/kind      # ~/.local/bin must be on your PATH

# macOS
brew install kind
```

Install kubectl (Linux and WSL; on macOS `brew install kubectl`):

```bash
curl -fsSLo ~/.local/bin/kubectl "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x ~/.local/bin/kubectl
```

On **Docker Desktop for Mac**, raise the memory under Settings → Resources (8 GB or more).
OrbStack gives Docker enough by default. On **Windows** (WSL 2 engine) there is no memory slider:
Docker gets what WSL gets, by default half your RAM, set in `.wslconfig`
([docs/setup.md](../../../../docs/setup.md#windows-through-wsl2)). On a 16 GB PC the default 8 GB
is just enough, but close heavy programs first: with the stack running, Windows was down to 1.5
GB free and WSL commands started timing out (see the table below).

**Apple Silicon (M1 to M4):** the official Hadoop image exists for x86 only, and a kind cluster
on an ARM Mac can't run x86 images. Everything else works; the two HDFS pods won't start, so the
HDFS sink connector fails and `make local-up` reports both. Use docker compose for HDFS on those
machines (it emulates x86).

## Start

From the repo root:

```bash
cp .env.example .env     # optional: put your keys in it (aisstream.io, DMI, webhook)
make local-up
```

The first start takes 10 to 20 minutes: it builds our Kafka Connect image (downloading every
plugin) and the cluster downloads about 5 GB of images. Later starts take a few minutes.

What it does, in this order. Each step prints what it did, and the first failing step stops the
script with the command to look closer:

1. Checks the tools and Docker's memory.
2. Creates the kind cluster `pf`, or reuses it if it exists.
3. Builds `pf/kafka-connect` and `pf/ais-producer`, tags them with the current git commit
   (`-dirty` if you have uncommitted changes) and loads them into kind. No registry is needed.
4. Creates the namespace `bd-gr-05` and, from `.env`, the Secrets.
5. Applies this overlay: every service, its volumes and its init Jobs.
6. Waits until every service is ready, and until `hdfs-init` and `cassandra-init` are done.
7. Wires the services: creates the Kafka topics, registers the Avro schemas, creates the ksqlDB
   queries, validates and deploys the connectors.
8. Runs the smoke test, which ends with `All services are up.`

Without `.env` it still starts everything. The AIS producer is then scaled to 0 (it can't connect
without a key), and the DMI and webhook connectors are skipped.

### Running it again

`make local-up` is safe to repeat. On a running cluster it updates instead of starting over:
changed manifests are applied, a rebuilt image replaces its pod, and every setup step skips what
already exists. Run it again after changing a manifest, a connector, a schema or a ksql file.

## Use it

kubectl needs to be pointed at the kind cluster first, once per terminal:

```bash
source infra/k8s/kind/env.sh     # this terminal now talks to kind, namespace bd-gr-05
kubectl get pods
```

`env.sh` writes a kubeconfig holding only the kind cluster and points this terminal at it, so
nothing you type here can reach the school cluster.

Look inside:

```bash
kubectl exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list
kubectl exec cassandra-0 -- cqlsh -e 'DESCRIBE KEYSPACE portcongestion'
kubectl exec deploy/redis -- redis-cli info keyspace
kubectl exec hdfs-namenode-0 -- hdfs dfs -ls /lake/raw
```

The web APIs, from your browser or curl (each keeps running until Ctrl+C):

```bash
kubectl port-forward svc/kafka-connect 8083    # http://localhost:8083/connectors?expand=status
kubectl port-forward svc/schema-registry 8081  # http://localhost:8081/subjects
kubectl port-forward svc/ksqldb 8088           # http://localhost:8088/info
kubectl port-forward svc/hdfs-namenode 9870    # HDFS web UI: http://localhost:9870
```

Check everything again at any time (read-only): `make local-check`.

### Loading DMA history files

The FilePulse connector reads CSV files from `/data/dma/incoming`, a volume in the Connect pod.
Copy one unzipped day in, and it starts reading within 30 seconds:

```bash
f=aisdk-2026-11-05.csv
kubectl exec -i deploy/kafka-connect -c kafka-connect -- \
  bash -c "cat > /data/dma/incoming/$f.part && mv /data/dma/incoming/$f.part /data/dma/incoming/$f" \
  < data/dma/incoming/$f
```

Why not `kubectl cp`: it needs `tar` inside the container, and the Confluent 8 Connect image has
none. The `.part` name keeps FilePulse from reading a half-copied file (it only lists `*.csv`).

What comes out, per CSV row: one record in `ais.history.raw`, then the ksqlDB history split
writes it to `ais.history.position` and, if the row carries real static data (IMO, destination
or ETA), to `ais.history.static`. Tested with a 3-row file in the DMA format: 3 raw, 3 positions,
2 statics (the third row had only `Unknown` static values).

One unzipped day is about 4.5 GB. The volume holds 10 GB, so delete processed files.

## Stop

```bash
make local-down     # deletes the cluster and ALL its data
```

To pause instead and keep the data, stop the container and start it again later:

```bash
docker stop pf-control-plane
docker start pf-control-plane     # the pods come back by themselves after a minute or two
```

## When something goes wrong

`make local-up` stops at the first step that fails and says what to look at. The usual causes:

| Symptom | Cause and fix |
|---|---|
| Pods stay `Pending` | Not enough memory for Docker. Give it 8 GB or more, then `make local-up` again. `kubectl describe pod <name>` shows "Insufficient memory". |
| A pod is `ImagePullBackOff` | The cluster can't download an image: check your internet connection. For `pf/...` images, run `make local-up` again (it reloads them). |
| `hdfs-*` pods crash with `exec format error` | Apple Silicon, see above. |
| A connector shows `FAILED` in the smoke test | `kubectl port-forward svc/kafka-connect 8083`, then open `http://localhost:8083/connectors/<name>/status`: the `trace` field has the reason. |
| `ksqlDB queries` fails | The topics or schemas aren't there yet. Run `make local-up` again; the step retries for 5 minutes. |
| WSL commands hang or fail with `Wsl/Service/WSAETIMEDOUT` (Windows) | Windows is out of memory; the stack takes about 6 GB inside WSL. Pause the cluster (`docker stop pf-control-plane`, data is kept), close other programs, then `docker start pf-control-plane`. |
| Anything else | `kubectl get pods`, then `kubectl describe pod <name>` (events at the bottom) and `kubectl logs <name>`. |

## Differences from the school cluster

| | Local (kind) | School |
|---|---|---|
| Start | `make local-up` | `make k8s-up` (see [../../README.md](../../README.md)) |
| Our images | built and loaded by `make local-up` | need a registry first, not chosen yet, so Connect and the producer don't run there yet |
| Storage | kind's `standard` class, deleted with the cluster | `hostpath-custom` |
| Memory requests | lowered in this overlay to fit a laptop | as in `base/` |
| Namespace | created by this overlay | exists already, we may not create it |

## Spark test jobs

Two small Jobs check that Spark runs as pods: `spark-pi` (Spark's own example) and
`smoke-count` (counts the AIS fixture in our image). `make local-up` doesn't run them.

### What happens when a Job runs

1. The Job starts a small **submitter** pod that only runs `spark-submit` in cluster mode.
2. `spark-submit` asks Kubernetes for a **driver** pod (`<name>-…-driver`): your program.
3. The driver asks for 2 **executor** pods, which do the work.
4. When the program ends, the driver and executor pods stay behind as `Completed`, so you can
   read their logs. They are not owned by the Job; see the clean-up commands below.

Shared settings (master, image, memory, 2 executors) are in [spark-defaults.conf](spark-defaults.conf).

### Build the image and load it into kind

From the repo root, with the cluster running:

```bash
docker build -f spark/Dockerfile --build-arg SPARK_IMAGE_TAG=4.2.0-scala2.13-java21-python3-ubuntu -t portcongestion/spark:4.2.0 .
kind load docker-image --name pf portcongestion/spark:4.2.0
```

Do both again after changing anything under `spark/`. (On OrbStack's own Kubernetes instead of
kind, the `docker build` alone is enough: it shares Docker's images.)

### Run a job

One at a time, so it fits on a laptop. After `source infra/k8s/kind/env.sh`:

```bash
kubectl apply -k infra/k8s/overlays/local -l spark-job=spark-pi
kubectl apply -k infra/k8s/overlays/local -l spark-job=smoke-count
kubectl get pods -w        # watch the pods come and go, Ctrl+C to stop
```

### Check the result

The Job only shows that `spark-submit` finished. **The result is the driver pod's phase**:
`Succeeded` means the program worked, `Failed` means it crashed.

```bash
kubectl get pods -l spark-role=driver -L spark-job
kubectl logs -l spark-role=driver,spark-job=smoke-count --tail=-1    # "rows 20", or "Pi is roughly ..."
kubectl logs job/smoke-count                                        # what spark-submit saw
```

### Run again, clean up

A Job runs once and can't be changed: delete it, then apply it again.

```bash
kubectl delete job smoke-count
kubectl delete pods -l spark-role=driver,spark-job=smoke-count   # its driver pod (and executors)
kubectl delete pods -l spark-role=driver                         # every old driver pod
```
