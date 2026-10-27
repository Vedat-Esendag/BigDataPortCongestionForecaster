"""F8, F9: the live job.

    ais.static  -> clean -> assign port -> features -> predict -> ship.prediction
                                                              \\-> outlook -> port.outlook

Reads live topics only. The `ais.history.*` topics are never read here, and nothing here writes
to Redis or Cassandra (the Kafka Connect sinks do that, and only for live topics).

Submit with `make spark-live`, or by hand:
    spark-submit --packages $SPARK_KAFKA_PACKAGE,$SPARK_AVRO_PACKAGE \\
        --py-files common.zip,streaming.zip streaming/live_job.py
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession

from common.avro_io import fetch_schema_id, read_topic, write_topic_batch
from common.clean import clean_positions, clean_static
from common.config import load_ports, load_thresholds
from common.features import build_features
from common.port_events import assign_destination_port, detect_port_events
from streaming.model_loader import load_latest_model
from streaming.outlook import build_outlook
from streaming.predictor import predict_eta

log = logging.getLogger("live_job")


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("port-congestion-live")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        # Prometheus scrapes the driver at :4040/metrics/prometheus
        .config(
            "spark.metrics.conf.*.sink.prometheusServlet.class",
            "org.apache.spark.metrics.sink.PrometheusServlet",
        )
        .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
        .getOrCreate()
    )


def build_prediction_stream(
    spark: SparkSession, bootstrap: str, starting_offsets: str, model
) -> DataFrame:
    """Wire the common functions together. Everything that matters happens inside them."""
    ports = load_ports()

    positions_raw = read_topic(spark, bootstrap, "ais.position", starting_offsets=starting_offsets)
    positions, _rejected_positions = clean_positions(positions_raw)
    # TODO(F4): write the rejected rows to the quarantine zone of the data lake.
    events = detect_port_events(positions, ports)

    static_raw = read_topic(spark, bootstrap, "ais.static", starting_offsets=starting_offsets)
    static, _rejected_static = clean_static(static_raw)
    static = assign_destination_port(static, ports)

    # TODO(F6): read weather.forecast and weather.observation and pass them as `weather`.
    features = build_features(static, positions=positions, events=events, weather=None)
    return predict_eta(features, model)


def make_batch_writer(bootstrap: str, registry_url: str):
    """foreachBatch function: write the predictions, then the outlook computed from them."""
    thresholds = load_thresholds()
    prediction_schema_id = fetch_schema_id(registry_url, "ship.prediction")
    outlook_schema_id = fetch_schema_id(registry_url, "port.outlook")

    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        batch.persist()
        write_topic_batch(batch, bootstrap, "ship.prediction", prediction_schema_id, key_col="mmsi")
        outlook = build_outlook(batch, thresholds)
        write_topic_batch(outlook, bootstrap, "port.outlook", outlook_schema_id, key_col="port")
        batch.unpersist()
        log.info("batch %s written", batch_id)

    return write


def main() -> None:
    parser = argparse.ArgumentParser(description="Port congestion live streaming job")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    parser.add_argument(
        "--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )
    parser.add_argument(
        "--checkpoint",
        default=os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints/live_job"),
        help="use an hdfs:// path outside of local demos",
    )
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    spark = build_session()
    model, version = load_latest_model(spark)
    log.info("model version: %s", version or "none (placeholder predictor)")

    predictions = build_prediction_stream(spark, args.bootstrap, args.starting_offsets, model)
    query = (
        predictions.writeStream.foreachBatch(make_batch_writer(args.bootstrap, args.registry))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
