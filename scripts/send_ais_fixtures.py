#!/usr/bin/env python3
"""Send the AIS test fixtures to Kafka as Confluent-framed Avro (test producer for S2).

    spark/tests/fixtures/ais_sample.csv         -> ais.position  (20 rows)
    spark/tests/fixtures/ais_static_sample.csv  -> ais.static    (6 rows)

Every row is sent as is, bad rows and duplicates included. Uses confluent-kafka's
AvroSerializer, the same library as services/ais-producer, with the schemas from
schemas/avro/*.avsc. The schemas must be registered first (make schemas); this script looks
them up and never registers anything. Key = MMSI as a string, like the real producer.

From the repo root, with the compose stack up (Kafka on localhost:9092):

    uv run --no-project --with 'confluent-kafka[avro,schemaregistry]' \
        python scripts/send_ais_fixtures.py
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "spark" / "tests" / "fixtures"
SCHEMAS = ROOT / "schemas" / "avro"
SOURCE = "fixture"


def _opt(value: str, cast):
    return cast(value) if value != "" else None


def _time(value: str) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def position_record(row: dict[str, str]) -> dict:
    event_time = _time(row["event_time"])
    return {
        "mmsi": int(row["mmsi"]),
        "event_time": event_time,
        "received_time": event_time,  # fixed, so a second send is an exact copy
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "sog": _opt(row["sog"], float),
        "cog": _opt(row["cog"], float),
        "heading": _opt(row["heading"], int),
        "rot": None,
        "nav_status": None,
        "message_type": None,
        "source": SOURCE,
    }


def static_record(row: dict[str, str]) -> dict:
    event_time = _time(row["event_time"])
    return {
        "mmsi": int(row["mmsi"]),
        "event_time": event_time,
        "received_time": event_time,
        "imo": _opt(row["imo"], int),
        "call_sign": row["call_sign"] or None,
        "name": row["name"] or None,
        "ship_type": _opt(row["ship_type"], int),
        "length_m": _opt(row["length_m"], float),
        "beam_m": _opt(row["beam_m"], float),
        "draught_m": _opt(row["draught_m"], float),
        "destination": row["destination"] or None,
        "eta_broadcast": _time(row["eta_broadcast"]),
        "source": SOURCE,
    }


FIXTURE_TOPICS = {
    "ais.position": (FIXTURES / "ais_sample.csv", position_record),
    "ais.static": (FIXTURES / "ais_static_sample.csv", static_record),
}


def records(topic: str) -> list[dict]:
    path, to_record = FIXTURE_TOPICS[topic]
    with path.open(newline="") as handle:
        return [to_record(row) for row in csv.DictReader(handle)]


def send(producer: Producer, registry: SchemaRegistryClient, topic: str) -> Counter:
    serializer = AvroSerializer(
        registry,
        (SCHEMAS / f"{topic}.avsc").read_text(),
        conf={"auto.register.schemas": False},
    )
    context = SerializationContext(topic, MessageField.VALUE)
    partitions: Counter = Counter()
    errors: list[str] = []

    def delivered(err, msg):
        if err is not None:
            errors.append(str(err))
        else:
            partitions[msg.partition()] += 1

    for record in records(topic):
        producer.produce(
            topic,
            key=str(record["mmsi"]),
            value=serializer(record, context),
            on_delivery=delivered,
        )
    producer.flush(30)
    if errors:
        raise SystemExit(f"{topic}: {len(errors)} message(s) failed: {errors[0]}")
    return partitions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--registry", default="http://localhost:8081")
    parser.add_argument(
        "--topic", action="append", choices=sorted(FIXTURE_TOPICS), help="default: both topics"
    )
    args = parser.parse_args()

    registry = SchemaRegistryClient({"url": args.registry})
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})
    for topic in args.topic or sorted(FIXTURE_TOPICS):
        partitions = send(producer, registry, topic)
        per_partition = ", ".join(f"p{p}={n}" for p, n in sorted(partitions.items()))
        print(f"{topic}: sent {sum(partitions.values())} ({per_partition})")


if __name__ == "__main__":
    main()
