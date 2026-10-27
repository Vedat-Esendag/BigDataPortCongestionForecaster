# Image for every Spark job (live streaming, training, analytics, evaluation).
# Build from the repo root:  docker build -f spark/Dockerfile --build-arg SPARK_IMAGE_TAG=... .
# The tag comes from versions.env; compose and the Makefile pass it in.
ARG SPARK_IMAGE_TAG=4.2.0-scala2.13-java21-python3-ubuntu
FROM apache/spark:${SPARK_IMAGE_TAG}

USER root
# PyYAML is the only Python dependency the jobs need beyond PySpark itself.
RUN pip3 install --no-cache-dir pyyaml

WORKDIR /opt/app
COPY spark /opt/app/spark
COPY config /opt/app/config
COPY schemas/avro /opt/app/schemas/avro
RUN mkdir -p spark/dist && cd spark && python3 -m zipfile -c dist/common.zip common \
    && python3 -m zipfile -c dist/streaming.zip streaming \
    && chown -R spark:spark /opt/app

USER spark
ENV CONFIG_DIR=/opt/app/config \
    AVRO_SCHEMA_DIR=/opt/app/schemas/avro
