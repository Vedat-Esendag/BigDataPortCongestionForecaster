# Port Congestion Forecaster

Big Data and Data Science Technology, SDU. A group project.

- Replaces the crew-typed ETA in AIS with a weather-aware predicted ETA per ship, with a 70% window.
- Rolls the ships up into a port outlook for the next 24 to 48 hours: late ships, arrival bunching, expected waiting at anchor.
- Live AIS comes from aisstream.io, history from the Danish Maritime Authority, weather from Open-Meteo and DMI.
- Kafka, Spark Structured Streaming and ksqlDB process it; HDFS, Cassandra and Redis store it.
- A planner watches it on a live web dashboard (FastAPI plus a React map).

Status: this is the scaffold. The AIS producer, schemas, infrastructure and the web skeleton are real; cleaning, port events,
features, training and the predictor are stubs marked `TODO(Fx)`. See [docs/decisions.md](docs/decisions.md) for what is real, what is not and what is still open.

## Prerequisites

| Tool | Version | Used for |
|------|---------|----------|
| git, make | any recent | version control, the `make` targets |
| Docker with Compose | recent, Compose v2 or newer (Docker Desktop on macOS and Windows, Docker Engine on Ubuntu) | the whole stack |
| [uv](https://docs.astral.sh/uv/) | any recent | Python projects and tools (installs Python 3.12 itself) |
| Java (Temurin) | 21 | running the Spark tests locally |
| Node | 24 LTS | the dashboard (`nvm install` in `services/dashboard` reads `.nvmrc`) |

Supported systems: **macOS, Ubuntu 22.04/24.04, and Windows 10/11 through WSL2** (you work inside
an Ubuntu terminal; plain PowerShell is not supported). The short versions are below, the full
guide with checks and fixes for common problems is in **[docs/setup.md](docs/setup.md)**.

<details>
<summary>macOS</summary>

```bash
xcode-select --install
brew install --cask docker            # or: brew install --cask orbstack
brew install uv
brew install --cask temurin@21
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

</details>

<details>
<summary>Ubuntu 22.04 / 24.04</summary>

```bash
sudo apt update && sudo apt install -y git make curl zip unzip python3
# Docker Engine: follow https://docs.docker.com/engine/install/ubuntu/ , then:
sudo usermod -aG docker $USER         # log out and in again
curl -LsSf https://astral.sh/uv/install.sh | sh
# Java 21 (Temurin): the Adoptium repository steps are in docs/setup.md, then:
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

</details>

<details>
<summary>Windows 10/11 (through WSL2)</summary>

In an administrator PowerShell:

```powershell
wsl --install                          # installs Ubuntu, then restart
winget install -e --id Docker.DockerDesktop
```

In Docker Desktop turn on "Use the WSL 2 based engine" and, under Resources > WSL Integration,
Ubuntu. Then open the **Ubuntu** terminal, clone the repository into your Linux home folder (not
`/mnt/c`), and follow the Ubuntu steps above **except** installing Docker Engine.

</details>

Node, on all three (the Ubuntu terminal on Windows):

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.8/install.sh | bash
# open a new terminal, then:
cd services/dashboard && nvm install
```

Check that it all works (`java -version` must say 21; the Makefile warns if `JAVA_HOME` points
somewhere else):

```bash
docker compose version && uv --version && java -version && node --version && make --version
```

You also need a free key from [aisstream.io](https://aisstream.io) for live data.
Versions of every component are in [versions.env](versions.env), and nowhere else.

## Quickstart

The commands are the same on every system. On Windows, run them in the Ubuntu (WSL) terminal.

```bash
cp .env.example .env            # then put your AISSTREAM_API_KEY in .env
make up-core                    # Kafka, Schema Registry, Kafka Connect, Redpanda Console
make topics schemas             # create the topics, register the Avro schemas
make run-producer               # live AIS into Kafka, running on your machine
```

Open Redpanda Console at <http://localhost:8085> and watch `ais.position` and `ais.static` fill up.

The rest of the stack, when you want it:

```bash
make up                         # all profiles: core, storage, apps, monitoring
make connectors                 # validate and deploy the Kafka Connect connectors
make spark-live                 # submit the live job (placeholder predictor for now)
```

| What | Where |
|------|-------|
| Dashboard | <http://localhost:8080> |
| API docs | <http://localhost:8000/docs> |
| Redpanda Console | <http://localhost:8085> |
| Grafana (admin / admin) | <http://localhost:3000> |
| Prometheus | <http://localhost:9090> |
| Spark UI (while a job runs) | <http://localhost:4040> |
| HDFS namenode | <http://localhost:9870> |

A subset is enough on a laptop: `docker compose` profiles are `core`, `storage`, `apps` and `monitoring`.
`make down` stops everything, `make down-volumes` also deletes the data.

Other targets: `make help` lists them all. The ones you will use most are `make test`, `make lint` and `make fmt`.

## How the repo is organized

```
config/            ports.yaml (everything port-specific), thresholds.yaml
schemas/avro/      one .avsc per Kafka topic: the contract between all components
services/
  ais-producer/    aisstream WebSocket to Kafka (Python, uv)
  api/             FastAPI: REST and WebSocket (Python, uv)
  dashboard/       React + TypeScript + Vite, MapLibre map, Recharts
spark/
  common/          cleaning, port events, features: shared by live and batch (the one copy)
  streaming/       the live job, placeholder predictor, outlook, model loader
  batch/           weather-delay analytics, forecast evaluation
  training/        ETA model training
  tests/           pytest with a local SparkSession
connect/           one JSON per Kafka Connect connector, and the Connect image
ksql/              alerts, and the split of raw DMA rows into the history topics
cassandra/         schema.cql
infra/             docker-compose, Kubernetes (Helm values and manifests), Prometheus and Grafana
scripts/           download_dma.py, create_topics.sh, register_schemas.py, deploy_connectors.sh
docs/              architecture, connectors, decisions
notebooks/         exploration
```

Start with [docs/architecture.md](docs/architecture.md). Requirement ids (F1 to F13) appear as
`TODO(Fx)` comments; `grep -rn "TODO(F" .` lists what is left to build.

Two rules the code relies on:

1. Cleaning, port events and features live **only** in `spark/common/`, and are imported by the live, batch and training jobs.
2. Historical replay uses the `ais.history.*` topics and never writes to Redis, the Cassandra outlook tables or alerts.

## Team conventions

- Work on a feature branch, never on `main`.
- Every change goes through a pull request that **one teammate reviews** before it is merged.
- **Never commit `.env`** or any key. Secrets come from the environment; `.env.example` holds names only.
- Run `make lint test` before you push. CI runs the same, plus a dashboard build and a compose check.
- Change a version in `versions.env`, not in five places. Change a schema in `schemas/avro/` and re-run `make schemas`.
