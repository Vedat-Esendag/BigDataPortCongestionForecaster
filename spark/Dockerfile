# Image for every Spark job (live streaming, training, analytics, evaluation).
# Build from the repo root:  docker build -f spark/Dockerfile --build-arg SPARK_IMAGE_TAG=... .
# The tag comes from versions.env; compose and the Makefile pass it in.
ARG SPARK_IMAGE_TAG=4.2.0-scala2.13-java21-python3-ubuntu
FROM apache/spark:${SPARK_IMAGE_TAG}

# Kafka and Avro connectors for Spark, baked in so jobs need no --packages and no internet at
# run time. Defaults repeat versions.env (spark/tests/test_versions.py checks); compose passes them.
ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
ARG SPARK_AVRO_PACKAGE=org.apache.spark:spark-avro_2.13:4.2.0

USER root
# PyYAML is the only Python dependency the jobs need beyond PySpark itself.
RUN pip3 install --no-cache-dir pyyaml

# Only for the in-cluster test sender (scripts/send_ais_fixtures.py, Job send-fixtures in
# infra/k8s/spark/school/one-off): Kafka is not reachable from a laptop, so the sender runs as a
# pod with this image. No Spark job imports it. Same version as services/ais-producer/uv.lock.
ARG CONFLUENT_KAFKA_VERSION=2.16.0
RUN pip3 install --no-cache-dir "confluent-kafka[avro,schemaregistry]==${CONFLUENT_KAFKA_VERSION}"

# Resolve the packages with Spark's own resolver (the same jar set --packages would download),
# then copy into /opt/spark/jars only the jars Spark does not already ship, so no class is on the
# classpath twice. The build log lists every jar as "added" or "skipped".
RUN set -eu; \
    echo "pass" > /tmp/noop.py; \
    /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/ivy \
        --packages "${SPARK_KAFKA_PACKAGE},${SPARK_AVRO_PACKAGE}" /tmp/noop.py; \
    for jar in /tmp/ivy/jars/*.jar; do \
        file="${jar##*/}"; file="${file#*_}"; artifact="${file%%-[0-9]*}"; \
        existing="$(ls /opt/spark/jars/"${artifact}"-[0-9]*.jar 2>/dev/null | head -n 1 || true)"; \
        if [ -n "${existing}" ]; then echo "skipped ${file} (Spark has ${existing##*/})"; \
        else cp "${jar}" "/opt/spark/jars/${file}"; echo "added   ${file}"; fi; \
    done; \
    rm -rf /tmp/ivy /tmp/noop.py

WORKDIR /opt/app
COPY spark /opt/app/spark
COPY config /opt/app/config
COPY schemas/avro /opt/app/schemas/avro
# The test sender; it finds the fixtures and schemas above relative to /opt/app.
COPY scripts/send_ais_fixtures.py /opt/app/scripts/send_ais_fixtures.py
RUN mkdir -p spark/dist && cd spark && python3 -m zipfile -c dist/common.zip common \
    && python3 -m zipfile -c dist/streaming.zip streaming \
    && chown -R spark:spark /opt/app

USER spark
ENV CONFIG_DIR=/opt/app/config \
    AVRO_SCHEMA_DIR=/opt/app/schemas/avro
