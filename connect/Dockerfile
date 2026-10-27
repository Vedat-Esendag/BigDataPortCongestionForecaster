# Kafka Connect with the plugins listed in docs/connectors.md.
# Build context is this folder; versions are passed in from versions.env by docker compose.
# Plugins are downloaded directly (not with confluent-hub) so the build is deterministic and does
# not depend on the CLI being present in the base image.
ARG CP_VERSION=8.4.0

FROM alpine:3.24 AS fetch
RUN apk add --no-cache curl unzip tar
WORKDIR /plugins

ARG FILEPULSE_VERSION
RUN curl -fsSL -o /tmp/p.zip "https://github.com/streamthoughts/kafka-connect-file-pulse/releases/download/v${FILEPULSE_VERSION}/streamthoughts-kafka-connect-file-pulse-${FILEPULSE_VERSION}.zip" \
    && unzip -q /tmp/p.zip -d /plugins && rm /tmp/p.zip

ARG CAMEL_HTTP_SOURCE_VERSION
RUN mkdir -p camel-http-source \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-http-source-kafka-connector/${CAMEL_HTTP_SOURCE_VERSION}/camel-http-source-kafka-connector-${CAMEL_HTTP_SOURCE_VERSION}-package.tar.gz" \
    | tar -xz -C camel-http-source

ARG HDFS_SINK_VERSION
RUN curl -fsSL -o /tmp/p.zip "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-hdfs/versions/${HDFS_SINK_VERSION}/confluentinc-kafka-connect-hdfs-${HDFS_SINK_VERSION}.zip" \
    && unzip -q /tmp/p.zip -d /plugins && rm /tmp/p.zip

ARG CASSANDRA_SINK_VERSION
RUN mkdir -p datastax-cassandra-sink \
    && curl -fsSL -o datastax-cassandra-sink/kafka-connect-cassandra-sink-${CASSANDRA_SINK_VERSION}.jar \
    "https://github.com/datastax/kafka-sink/releases/download/${CASSANDRA_SINK_VERSION}/kafka-connect-cassandra-sink-${CASSANDRA_SINK_VERSION}.jar"

ARG REDIS_SINK_VERSION
RUN curl -fsSL -o /tmp/p.zip "https://github.com/redis-field-engineering/redis-kafka-connect/releases/download/v${REDIS_SINK_VERSION}/redis-redis-kafka-connect-${REDIS_SINK_VERSION}.zip" \
    && unzip -q /tmp/p.zip -d /plugins && rm /tmp/p.zip

ARG LENSES_HTTP_SINK_VERSION
RUN curl -fsSL -o /tmp/p.zip "https://github.com/lensesio/stream-reactor/releases/download/${LENSES_HTTP_SINK_VERSION}/kafka-connect-http-${LENSES_HTTP_SINK_VERSION}.zip" \
    && unzip -q /tmp/p.zip -d /plugins && rm /tmp/p.zip

FROM confluentinc/cp-kafka-connect:${CP_VERSION}
COPY --from=fetch /plugins /usr/share/connect-plugins
