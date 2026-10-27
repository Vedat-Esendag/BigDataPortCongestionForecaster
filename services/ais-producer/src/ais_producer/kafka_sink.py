"""Avro-serialize mapped records with Schema Registry and produce them to Kafka."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ais_producer import metrics
from ais_producer.config import Settings
from ais_producer.mapping import Mapped

log = logging.getLogger(__name__)


class AvroSink:
    def __init__(self, settings: Settings) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "client.id": "ais-producer",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        # By default the schema must already be registered (make schemas) so that schemas/avro
        # stays the single contract. Set SCHEMA_AUTO_REGISTER=true to let the producer register.
        serializer_conf = {
            "auto.register.schemas": settings.auto_register_schemas,
            "use.latest.version": not settings.auto_register_schemas,
        }
        self._serializers = {
            topic: AvroSerializer(
                registry,
                (settings.schema_dir / f"{topic}.avsc").read_text(),
                conf=serializer_conf,
            )
            for topic in (settings.position_topic, settings.static_topic)
        }

    def send(self, mapped: Mapped) -> None:
        """Serialize and queue one record. Raises on serialization errors, never on delivery."""
        context = SerializationContext(mapped.topic, MessageField.VALUE)
        value = self._serializers[mapped.topic](mapped.value, context)
        while True:
            try:
                self._producer.produce(
                    topic=mapped.topic,
                    key=mapped.key.encode(),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Local queue full: let librdkafka drain it, then try again.
                self._producer.poll(1.0)
        self._producer.poll(0)

    @staticmethod
    def _on_delivery(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            metrics.PRODUCE_ERRORS.labels(stage="delivery").inc()
            log.error("delivery failed for %s: %s", message.topic(), error)
        else:
            metrics.MESSAGES_PRODUCED.labels(topic=message.topic()).inc()

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for queued messages. Returns how many are still undelivered."""
        return self._producer.flush(timeout)
