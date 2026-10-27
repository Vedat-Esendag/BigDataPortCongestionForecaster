"""Read and write Confluent-framed Avro on Kafka topics with Spark's built-in Avro functions.

Confluent's serializers prepend 5 bytes to every Avro payload: a zero magic byte and the 4-byte
big-endian Schema Registry id. Spark's `from_avro` does not understand that header, so:
  - reading strips the first 5 bytes and decodes with the schema from schemas/avro/<topic>.avsc
  - writing encodes with the same file and prepends the id fetched from Schema Registry

KNOWN LIMIT: this ignores schema evolution. The schema in the .avsc file is used as both writer
and reader schema, and the id in the header is never looked up on read. If a producer changes a
schema, update the file and redeploy the job. See docs/decisions.md.
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

MAGIC_BYTE = 0
HEADER_LENGTH = 5


def schema_dir() -> Path:
    override = os.environ.get("AVRO_SCHEMA_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "schemas" / "avro"


def load_avsc(topic: str) -> str:
    """Schema text for a topic, from schemas/avro/<topic>.avsc."""
    return (schema_dir() / f"{topic}.avsc").read_text()


def field_names(avsc: str) -> list[str]:
    return [field["name"] for field in json.loads(avsc)["fields"]]


def fetch_schema_id(registry_url: str, topic: str) -> int:
    """Id of the latest registered value schema of a topic."""
    url = f"{registry_url.rstrip('/')}/subjects/{topic}-value/versions/latest"
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)["id"]


def decode_confluent_avro(df: DataFrame, avsc: str, *, binary_col: str = "value") -> DataFrame:
    """Strip the 5-byte header from `binary_col` and decode it into one column per Avro field."""
    payload = F.expr(f"substring({binary_col}, {HEADER_LENGTH + 1}, length({binary_col}))")
    decoded = from_avro(payload, avsc, {"mode": "PERMISSIVE"})
    return df.select(
        F.col("key").cast("string").alias("key"), decoded.alias("record"), "timestamp"
    ).select("key", F.col("timestamp").alias("kafka_timestamp"), "record.*")


def read_topic(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    *,
    streaming: bool = True,
    starting_offsets: str = "latest",
) -> DataFrame:
    """Read an Avro topic as a DataFrame with columns key, kafka_timestamp and the Avro fields."""
    reader = spark.readStream if streaming else spark.read
    options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
    }
    raw = reader.format("kafka").options(**options).load()
    return decode_confluent_avro(raw, load_avsc(topic))


def encode_confluent_avro(df: DataFrame, avsc: str, schema_id: int, *, key_col: str) -> DataFrame:
    """Build the `key` and `value` columns the Kafka sink expects from a DataFrame of records."""
    names = field_names(avsc)
    header = bytearray([MAGIC_BYTE]) + bytearray(schema_id.to_bytes(4, "big"))
    avro = to_avro(F.struct(*names), avsc)
    return df.select(
        F.col(key_col).cast("string").alias("key"),
        F.concat(F.lit(header), avro).alias("value"),
    )


def write_topic_batch(
    df: DataFrame,
    bootstrap_servers: str,
    topic: str,
    schema_id: int,
    *,
    key_col: str,
) -> None:
    """Write a (non-streaming) DataFrame to an Avro topic. Use it inside foreachBatch too."""
    encoded = encode_confluent_avro(df, load_avsc(topic), schema_id, key_col=key_col)
    (
        encoded.write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .save()
    )
