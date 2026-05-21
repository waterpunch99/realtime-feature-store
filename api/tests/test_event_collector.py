from datetime import UTC, datetime

import pytest

from app.kafka_client import KafkaPublishResult
from app.services.event_collector import EventCollectorFailure, EventCollectorService
from app.services.event_validation import EventValidationService
from app.services.metrics_service import MetricsService


class _QualityLogRepository:
    def __init__(self) -> None:
        self.records = []

    async def add_event_quality_log(self, **kwargs) -> None:
        self.records.append(kwargs)


class _KafkaClient:
    async def publish_json(self, topic: str, key: str, value: dict):
        self.topic = topic
        self.key = key
        self.value = value
        return KafkaPublishResult(topic=topic, partition=0, offset=1)


def _collector(quality_repo: _QualityLogRepository, kafka_client: _KafkaClient):
    return EventCollectorService(
        validation_service=EventValidationService(),
        quality_log_repository=quality_repo,
        kafka_client=kafka_client,
        metrics_service=MetricsService(),
        raw_topic="raw-user-events",
    )


def _event() -> dict:
    return {
        "event_type": "click",
        "user_id": "u_1",
        "session_id": "s_1",
        "product_id": "p_1",
        "category_id": "c_1",
        "event_time": datetime.now(UTC).isoformat(),
        "properties": {},
    }


@pytest.mark.asyncio
async def test_collect_generates_event_id_and_publishes_to_kafka() -> None:
    quality_repo = _QualityLogRepository()
    kafka_client = _KafkaClient()

    response = await _collector(quality_repo, kafka_client).collect(_event())

    assert response.accepted is True
    assert response.event_id
    assert kafka_client.topic == "raw-user-events"
    assert kafka_client.key == "u_1"
    assert kafka_client.value["ingest_time"] is not None
    assert quality_repo.records == []


@pytest.mark.asyncio
async def test_collect_invalid_event_writes_quality_log() -> None:
    quality_repo = _QualityLogRepository()
    kafka_client = _KafkaClient()
    event = _event()
    event.pop("product_id")

    with pytest.raises(EventCollectorFailure) as exc_info:
        await _collector(quality_repo, kafka_client).collect(event)

    assert exc_info.value.reason_code == "required_field_missing"
    assert quality_repo.records[0]["event_id"]
    assert quality_repo.records[0]["reason_code"] == "required_field_missing"

