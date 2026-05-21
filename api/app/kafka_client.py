import asyncio
import json
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer

from app.config import Settings


@dataclass(frozen=True)
class KafkaProducerConfig:
    bootstrap_servers: str
    client_id: str
    request_timeout_seconds: float


@dataclass(frozen=True)
class KafkaPublishResult:
    topic: str
    partition: int
    offset: int


class KafkaPublishError(RuntimeError):
    pass


class KafkaProducerClient:
    """Shared Kafka producer wrapper with delivery confirmation."""

    def __init__(self, settings: Settings):
        self.config = KafkaProducerConfig(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
            request_timeout_seconds=settings.kafka_request_timeout_seconds,
        )
        self._producer = Producer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "client.id": self.config.client_id,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "delivery.timeout.ms": int(self.config.request_timeout_seconds * 1000),
            }
        )

    @property
    def producer(self) -> Producer:
        return self._producer

    async def publish_json(self, topic: str, key: str, value: dict[str, Any]) -> KafkaPublishResult:
        return await asyncio.to_thread(self._publish_json_sync, topic, key, value)

    def _publish_json_sync(self, topic: str, key: str, value: dict[str, Any]) -> KafkaPublishResult:
        delivery_result: dict[str, Any] = {}

        def delivery_callback(error, message) -> None:
            delivery_result["error"] = error
            delivery_result["message"] = message

        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode(
            "utf-8"
        )

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=payload,
                callback=delivery_callback,
            )
            remaining = self._producer.flush(self.config.request_timeout_seconds)
        except BufferError as exc:
            raise KafkaPublishError("Kafka producer queue is full") from exc
        except Exception as exc:
            raise KafkaPublishError(str(exc)) from exc

        if remaining > 0:
            raise KafkaPublishError("Kafka delivery timed out")

        error = delivery_result.get("error")
        if error is not None:
            raise KafkaPublishError(str(error))

        message = delivery_result.get("message")
        if message is None:
            raise KafkaPublishError("Kafka delivery callback did not return metadata")

        return KafkaPublishResult(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )

    async def close(self) -> None:
        await asyncio.to_thread(self._producer.flush, 5)

    async def healthcheck(self) -> bool:
        try:
            metadata = await asyncio.to_thread(
                self._producer.list_topics,
                None,
                self.config.request_timeout_seconds,
            )
            return len(metadata.brokers) > 0
        except Exception:
            return False
