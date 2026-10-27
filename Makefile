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
        compose-config run-producer spark-package spark-live dashboard-build download-dma clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

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

spark-live: spark-package ## Submit the live streaming job inside the spark container
	$(COMPOSE) --profile apps exec spark /opt/spark/bin/spark-submit \
		--packages $(SPARK_KAFKA_PACKAGE),$(SPARK_AVRO_PACKAGE) \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--py-files /opt/app/spark/dist/common.zip,/opt/app/spark/dist/streaming.zip \
		/opt/app/spark/streaming/live_job.py

dashboard-build: ## Type-check and build the dashboard
	cd services/dashboard && npm ci && npm run build

download-dma: ## Download DMA files: make download-dma START=2026-09-01 END=2026-09-02
	python3 scripts/download_dma.py --start $(START) --end $(END)

clean: ## Remove build and cache directories
	rm -rf spark/dist services/dashboard/dist .ruff_cache .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
