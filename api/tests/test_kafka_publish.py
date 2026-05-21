import pytest

from app.kafka_client import KafkaProducerClient, KafkaProducerConfig


class _Message:
    def topic(self) -> str:
        return "raw-user-events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 42


class _Producer:
    def produce(self, *, topic, key, value, callback) -> None:
        self.topic = topic
        self.key = key
        self.value = value
        self.callback = callback

    def flush(self, timeout) -> int:
        self.callback(None, _Message())
        return 0


@pytest.mark.asyncio
async def test_publish_json_waits_for_delivery_metadata() -> None:
    client = object.__new__(KafkaProducerClient)
    client.config = KafkaProducerConfig(
        bootstrap_servers="localhost:19092",
        client_id="test",
        request_timeout_seconds=1,
    )
    client._producer = _Producer()

    result = await client.publish_json(
        topic="raw-user-events",
        key="u_1",
        value={"event_id": "e_1"},
    )

    assert result.topic == "raw-user-events"
    assert result.partition == 0
    assert result.offset == 42
    assert client._producer.topic == "raw-user-events"
    assert client._producer.key == b"u_1"
