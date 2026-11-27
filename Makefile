# Port congestion forecaster. Run `make help` for the list of targets.
# Versions come from versions.env (one place), secrets from .env (never committed).

include versions.env

COMPOSE := docker compose -f infra/docker-compose.yml
ALL_PROFILES := --profile core --profile storage --profile apps --profile monitoring
PY_PROJECTS := services/ais-producer services/api spark

# Spark 4 needs Java 17 or newer; the project targets $(JAVA_VERSION). Keep a JAVA_HOME that is
# already new enough, otherwise ask macOS for a JDK. java_home can silently return another
# version when the requested one is missing, so the real version is always read from the JDK.
java_major = $(shell sed -n 's/^JAVA_VERSION="\([0-9]*\).*/\1/p' "$(1)/release" 2>/dev/null)
ifneq ($(shell [ "$(call java_major,$(JAVA_HOME))" -ge 17 ] 2>/dev/null && echo ok),ok)
JAVA_HOME := $(shell /usr/libexec/java_home -v $(JAVA_VERSION) 2>/dev/null)
endif
JAVA_MAJOR := $(call java_major,$(JAVA_HOME))

.PHONY: help up up-core down down-volumes topics schemas connectors test lint fmt \
        compose-config run-producer spark-package spark-live spark-lake spark-image spark-push images-push images-build dashboard-build clean \
        k8s-diff k8s-apply k8s-check k8s-up k8s-secrets k8s-test-hdfs k8s-test-ksqldb k8s-hdfs-report \
        local-up local-down local-check local-test-ksqldb \
        r1-package r1-up r1-ingest r1-order r1-arrivals r1-compare

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

# --- Stack ---------------------------------------------------------------------------------

up: ## Start every profile (core, storage, apps, monitoring)
	@mkdir -p data/dma
	$(COMPOSE) $(ALL_PROFILES) up -d --build

up-core: ## Start Kafka, Schema Registry, Connect and Redpanda Console
	@mkdir -p data/dma
	$(COMPOSE) --profile core up -d --build --wait

down: ## Stop everything, keep volumes
	$(COMPOSE) --profile '*' down

down-volumes: ## Stop everything and delete all volumes (wipes Kafka, HDFS, Cassandra data)
	$(COMPOSE) --profile '*' down -v

compose-config: ## Validate the compose file with every profile enabled
	$(COMPOSE) --profile '*' config --quiet && echo "compose config OK"

k8s-diff: ## Show what k8s-apply would change on the school cluster (namespace bd-gr-05)
	infra/k8s/deploy.sh diff

k8s-apply: ## Deploy infra/k8s/overlays/school to the school cluster (namespace bd-gr-05 only)
	infra/k8s/deploy.sh apply

k8s-check: ## Smoke test: check every service on the school cluster answers (read-only)
	infra/k8s/check.sh

k8s-up: k8s-apply k8s-check ## Deploy to the school cluster and wait until every service answers

k8s-test-hdfs: ## Restart the HDFS pods and check a test file survives (HDFS down 1-2 min)
	infra/k8s/test-hdfs-restart.sh

k8s-test-ksqldb: ## Restart the ksqlDB pod and check its queries come back (ksqlDB down ~30 s)
	infra/k8s/test-ksqldb-restart.sh

k8s-hdfs-report: ## HDFS health: datanodes, free space, block problems, size per lake zone (read-only)
	infra/k8s/hdfs-report.sh

k8s-secrets: ## Create or update the Kubernetes Secrets from the keys in .env
	scripts/create_k8s_secrets.sh

local-up: ## The whole stack on a local kind cluster, in one command (infra/k8s/overlays/local/README.md)
	infra/k8s/local-up.sh up

local-check: ## Smoke test of the local kind cluster (read-only)
	PF_CLUSTER=local infra/k8s/check.sh

local-test-ksqldb: ## Restart test of ksqlDB on the local kind cluster
	PF_CLUSTER=local infra/k8s/test-ksqldb-restart.sh

local-down: ## Delete the local kind cluster and all its data
	infra/k8s/local-up.sh down

topics: ## Create the Kafka topics
	scripts/create_topics.sh

schemas: ## Register the Avro schemas in Schema Registry
	python3 scripts/register_schemas.py

connectors: ## Validate and deploy the Kafka Connect connectors
	scripts/deploy_connectors.sh

# --- Quality -------------------------------------------------------------------------------

test: ## Run the Python test suites (Spark tests need Java $(JAVA_VERSION))
	@if [ "$(JAVA_MAJOR)" != "$(JAVA_VERSION)" ]; then \
		echo "WARNING: the project targets Java $(JAVA_VERSION), but no JDK $(JAVA_VERSION) was found (JAVA_HOME='$(JAVA_HOME)', Java '$(JAVA_MAJOR)')."; \
		echo "         Spark also runs on 17. To install $(JAVA_VERSION) and set JAVA_HOME, see docs/setup.md"; \
	fi
	@set -e; for project in $(PY_PROJECTS); do \
		echo "== pytest $$project"; \
		(cd $$project && $(if $(JAVA_HOME),JAVA_HOME="$(JAVA_HOME)") uv run pytest); \
	done
	@echo "== pytest scripts and connector rules"
	@uv run --no-project --with pytest --with pyyaml pytest scripts/tests -q

lint: ## ruff check and format check
	uvx ruff@$(RUFF_VERSION) check .
	uvx ruff@$(RUFF_VERSION) format --check .

fmt: ## Auto-fix lint problems and format the code
	uvx ruff@$(RUFF_VERSION) check --fix .
	uvx ruff@$(RUFF_VERSION) format .

# --- Running pieces ------------------------------------------------------------------------

run-producer: ## Run the AIS producer on the host (needs AISSTREAM_API_KEY in .env)
	cd services/ais-producer && uv run --env-file ../../.env python -m ais_producer

spark-package: ## Zip spark/common and spark/streaming for spark-submit --py-files
	mkdir -p spark/dist
	rm -f spark/dist/common.zip spark/dist/streaming.zip
	cd spark && zip -rq dist/common.zip common -x '*/__pycache__/*'
	cd spark && zip -rq dist/streaming.zip streaming -x '*/__pycache__/*'

# Settings for Spark jobs on the compose stack. Its HDFS has one datanode, so write one copy of
# each block (the client default is 3, which leaves every block under-replicated).
SPARK_COMPOSE_CONF := --conf spark.hadoop.dfs.replication=1

spark-live: spark-package ## Submit the live streaming job inside the spark container
	$(COMPOSE) --profile apps exec spark /opt/spark/bin/spark-submit $(SPARK_COMPOSE_CONF) \
		--py-files /opt/app/spark/dist/common.zip,/opt/app/spark/dist/streaming.zip \
		/opt/app/spark/streaming/live_job.py

TOPIC ?= ais.position
LAKE_ARGS ?=
spark-lake: spark-package ## Record a topic in the raw lake zone: make spark-lake TOPIC=ais.static
	$(COMPOSE) --profile apps exec spark /opt/spark/bin/spark-submit $(SPARK_COMPOSE_CONF) \
		--py-files /opt/app/spark/dist/common.zip \
		/opt/app/spark/streaming/kafka_to_lake.py --topic $(TOPIC) $(LAKE_ARGS)

# --- R1: the October Esbjerg waiting analysis in Spark (docs/spark-milestones.md) ----------
# PORTAREA_DIR: the folder with portarea_<day>.csv.gz, year_comparison.json and year/, mounted
# read-only (infra/compose.r1.yml). R1_BLOCKS: blocks of the day list, empty = all 84 days.
export PORTAREA_DIR
R1_BLOCKS ?=
R1_DRIVER_MEMORY ?= 6g
R1_COMPOSE := $(COMPOSE) -f infra/compose.r1.yml
R1_BLOCK_ARGS = $(if $(strip $(R1_BLOCKS)),--blocks $(R1_BLOCKS))
R1_SUBMIT = $(R1_COMPOSE) --profile apps exec spark /opt/spark/bin/spark-submit $(SPARK_COMPOSE_CONF) \
	--driver-memory $(R1_DRIVER_MEMORY) --conf spark.sql.session.timeZone=UTC \
	--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
	--py-files /opt/app/spark/dist/common.zip,/opt/app/spark/dist/jobs.zip

r1-package: spark-package
	cd spark && rm -f dist/jobs.zip && zip -rq dist/jobs.zip jobs -x '*/__pycache__/*'

r1-up: ## R1: start HDFS and the Spark container with PORTAREA_DIR mounted read-only
	$(R1_COMPOSE) --profile core --profile storage --profile apps up -d --build namenode datanode hdfs-init spark

r1-ingest: r1-package ## R1a: portarea CSVs -> /lake/standardized + /lake/quarantine (R1_BLOCKS=0 for one block)
	$(R1_SUBMIT) /opt/app/spark/jobs/ingest_portarea.py $(R1_BLOCK_ARGS)

R1_ORDER_DAYS ?= 2025-11-08 2026-10-14
r1-order: r1-package ## R1a check: SHA-256 of the row order, Spark vs plain Python (R1_ORDER_DAYS)
	$(R1_SUBMIT) /opt/app/spark/jobs/check_source_order.py --days $(R1_ORDER_DAYS)

r1-arrivals: r1-package ## R1b: arrivals, stops and ETA at entry -> /lake/curated
	$(R1_SUBMIT) /opt/app/spark/jobs/esbjerg_arrivals.py $(R1_BLOCK_ARGS)

R1_COMPARE_ARGS ?=
r1-compare: r1-package ## R1c: event-level comparison with the October output
	$(R1_SUBMIT) /opt/app/scripts/compare_r1.py $(R1_BLOCK_ARGS) $(R1_COMPARE_ARGS)

# The Spark image for Kubernetes (infra/k8s/spark), for both node types: amd64 (school) and arm64
# (Apple Silicon). Tagged with the commit; "-dirty" when the tree has uncommitted or untracked
# files, so a tag never claims to be a commit it is not. spark-image loads it into the local
# Docker only; spark-push builds the amd64 variant and pushes it to $(SPARK_REGISTRY).
SPARK_JOBS_IMAGE := spark-jobs
SPARK_JOBS_TAG = $(shell git rev-parse --short HEAD)$(shell [ -n "$$(git status --porcelain)" ] && echo -dirty)
SPARK_PLATFORMS ?= linux/amd64,linux/arm64
REGISTRY ?= ghcr.io/vedat-esendag
SPARK_REGISTRY ?= $(REGISTRY)
SPARK_BUILD_ARGS = \
		--build-arg SPARK_IMAGE_TAG=$(SPARK_IMAGE_TAG) \
		--build-arg SPARK_KAFKA_PACKAGE=$(SPARK_KAFKA_PACKAGE) \
		--build-arg SPARK_AVRO_PACKAGE=$(SPARK_AVRO_PACKAGE)

spark-image: ## Build spark-jobs:<git sha> for amd64 and arm64 into local Docker (no push)
	@tag="$(SPARK_JOBS_TAG)"; \
	case "$$tag" in *-dirty) echo "warning: uncommitted changes, tagging $(SPARK_JOBS_IMAGE):$$tag";; esac; \
	docker buildx build --platform $(SPARK_PLATFORMS) --load \
		$(SPARK_BUILD_ARGS) \
		-f spark/Dockerfile -t $(SPARK_JOBS_IMAGE):$$tag . && \
	echo "built $(SPARK_JOBS_IMAGE):$$tag ($(SPARK_PLATFORMS))"

# Only linux/amd64: the school node. Refuses a dirty tree, so a public tag always names a commit.
# Needs `docker login ghcr.io` first; the package must be made public once on GitHub.
spark-push: ## Build spark-jobs:<git sha> for linux/amd64, push to SPARK_REGISTRY (clean tree only)
	@tag="$(SPARK_JOBS_TAG)"; \
	case "$$tag" in *-dirty) echo "error: uncommitted or untracked files (git status); commit or move them, then push again" >&2; exit 1;; esac; \
	docker buildx build --platform linux/amd64 --push \
		$(SPARK_BUILD_ARGS) \
		-f spark/Dockerfile -t $(SPARK_REGISTRY)/$(SPARK_JOBS_IMAGE):$$tag . && \
	echo "pushed $(SPARK_REGISTRY)/$(SPARK_JOBS_IMAGE):$$tag (linux/amd64)"

# All our images for the school cluster (one amd64 node), each pushed as $(REGISTRY)/<name>:<git
# sha>, the same tag for all. Build args and contexts are the ones docker compose uses
# (infra/compose.services.yml) and local-up.sh uses, from versions.env. Pick a subset with
# IMAGES=, for example: make images-push IMAGES="spark-jobs pf/ais-producer"
IMAGES ?= spark-jobs pf/ais-producer pf/kafka-connect pf/api pf/dashboard
PRODUCER_BUILD_ARGS = \
		--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
		--build-arg UV_VERSION=$(UV_VERSION)
CONNECT_BUILD_ARGS = \
		--build-arg CP_VERSION=$(CP_VERSION) \
		--build-arg FILEPULSE_VERSION=$(FILEPULSE_VERSION) \
		--build-arg CAMEL_HTTP_SOURCE_VERSION=$(CAMEL_HTTP_SOURCE_VERSION) \
		--build-arg HDFS_SINK_VERSION=$(HDFS_SINK_VERSION) \
		--build-arg CASSANDRA_SINK_VERSION=$(CASSANDRA_SINK_VERSION) \
		--build-arg REDIS_SINK_VERSION=$(REDIS_SINK_VERSION) \
		--build-arg LENSES_HTTP_SINK_VERSION=$(LENSES_HTTP_SINK_VERSION)
API_BUILD_ARGS = \
		--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
		--build-arg UV_VERSION=$(UV_VERSION)
DASHBOARD_BUILD_ARGS = \
		--build-arg NODE_VERSION=$(NODE_VERSION)
# Per image: build args, Dockerfile and build context.
IMAGE_BUILD_spark-jobs       = $(SPARK_BUILD_ARGS) -f spark/Dockerfile .
IMAGE_BUILD_pf/ais-producer  = $(PRODUCER_BUILD_ARGS) -f services/ais-producer/Dockerfile .
IMAGE_BUILD_pf/kafka-connect = $(CONNECT_BUILD_ARGS) -f connect/Dockerfile connect
IMAGE_BUILD_pf/api           = $(API_BUILD_ARGS) -f services/api/Dockerfile .
IMAGE_BUILD_pf/dashboard     = $(DASHBOARD_BUILD_ARGS) -f services/dashboard/Dockerfile services/dashboard
image_build = $(if $(IMAGE_BUILD_$(1)),$(IMAGE_BUILD_$(1)),$(error unknown image "$(1)"; known: spark-jobs pf/ais-producer pf/kafka-connect pf/api pf/dashboard))

images-push: ## Build IMAGES for linux/amd64, push each to REGISTRY with the git sha (clean tree only)
	@tag="$(SPARK_JOBS_TAG)"; \
	case "$$tag" in *-dirty) echo "error: uncommitted or untracked files (git status); commit or move them, then push again" >&2; exit 1;; esac; \
	$(foreach i,$(IMAGES),echo "== $(REGISTRY)/$(i):$$tag"; \
	docker buildx build --platform linux/amd64 --push $(call image_build,$(i)) -t $(REGISTRY)/$(i):$$tag || exit 1; \
	echo "pushed $(REGISTRY)/$(i):$$tag (linux/amd64)"; ) \
	echo "done: $(IMAGES) at $$tag"

images-build: ## Build IMAGES for linux/amd64 into local Docker as <name>:<git sha>-amd64 (no push)
	@tag="$(SPARK_JOBS_TAG)-amd64"; \
	$(foreach i,$(IMAGES),echo "== $(i):$$tag"; \
	docker buildx build --platform linux/amd64 --load $(call image_build,$(i)) -t $(i):$$tag || exit 1; ) \
	echo "built $(IMAGES) at $$tag"

dashboard-build: ## Type-check and build the dashboard
	cd services/dashboard && npm ci && npm run build

clean: ## Remove build and cache directories
	rm -rf spark/dist services/dashboard/dist .ruff_cache .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
