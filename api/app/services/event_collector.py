import logging
from typing import Any
from uuid import uuid4

from app.kafka_client import KafkaProducerClient, KafkaPublishError
from app.repositories.quality_log_repository import QualityLogRepository
from app.schemas.event import EventIngestResponse
from app.services.event_validation import EventValidationError, EventValidationService
from app.services.metrics_service import MetricsService

logger = logging.getLogger(__name__)


class EventCollectorFailure(Exception):
    def __init__(
        self,
        *,
        reason_code: str,
        reason_detail: str,
        event_id: str | None = None,
        status_code: int = 400,
    ):
        self.reason_code = reason_code
        self.reason_detail = reason_detail
        self.event_id = event_id
        self.status_code = status_code
        super().__init__(reason_detail)


class EventCollectorService:
    def __init__(
        self,
        *,
        validation_service: EventValidationService,
        quality_log_repository: QualityLogRepository,
        kafka_client: KafkaProducerClient,
        metrics_service: MetricsService,
        raw_topic: str,
    ):
        self._validation_service = validation_service
        self._quality_log_repository = quality_log_repository
        self._kafka_client = kafka_client
        self._metrics_service = metrics_service
        self._raw_topic = raw_topic

    async def collect(self, raw_event: dict[str, Any]) -> EventIngestResponse:
        raw_event = self._ensure_event_id(raw_event)

        try:
            validated = self._validation_service.validate_and_enrich(raw_event)
        except EventValidationError as exc:
            self._metrics_service.increment("validation_failure_total")
            self._metrics_service.increment("event_ingest_failure_total")
            await self._quality_log_repository.add_event_quality_log(
                event_id=self._extract_text(raw_event, "event_id"),
                event_type=self._extract_text(raw_event, "event_type"),
                user_id=self._extract_text(raw_event, "user_id"),
                reason_code=exc.reason_code,
                reason_detail=exc.reason_detail,
                raw_event=raw_event,
            )
            logger.info(
                "event_validation_failed",
                extra={
                    "event_id": self._extract_text(raw_event, "event_id"),
                    "reason_code": exc.reason_code,
                    "reason_detail": exc.reason_detail,
                },
            )
            raise EventCollectorFailure(
                reason_code=exc.reason_code,
                reason_detail=exc.reason_detail,
                event_id=self._extract_text(raw_event, "event_id"),
                status_code=400,
            ) from exc

        try:
            publish_result = await self._kafka_client.publish_json(
                topic=self._raw_topic,
                key=validated.kafka_key,
                value=validated.payload,
            )
        except KafkaPublishError as exc:
            self._metrics_service.increment("kafka_publish_failure_total")
            self._metrics_service.increment("event_ingest_failure_total")
            logger.error(
                "kafka_publish_failed",
                extra={
                    "event_id": validated.event.event_id,
                    "topic": self._raw_topic,
                    "reason_detail": str(exc),
                },
            )
            raise EventCollectorFailure(
                reason_code="kafka_publish_failed",
                reason_detail=str(exc),
                event_id=validated.event.event_id,
                status_code=503,
            ) from exc

        self._metrics_service.increment("kafka_publish_success_total")
        self._metrics_service.increment("event_ingest_success_total")
        logger.info(
            "event_published",
            extra={
                "event_id": validated.event.event_id,
                "event_type": validated.event.event_type,
                "user_id": validated.event.user_id,
                "topic": publish_result.topic,
                "partition": publish_result.partition,
                "offset": publish_result.offset,
            },
        )

        return EventIngestResponse(
            accepted=True,
            event_id=validated.event.event_id or "",
            topic=publish_result.topic,
            partition=publish_result.partition,
            offset=publish_result.offset,
        )

    def _extract_text(self, raw_event: dict[str, Any], field_name: str) -> str | None:
        value = raw_event.get(field_name)
        if value is None:
            return None
        return str(value)

    def _ensure_event_id(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        if raw_event.get("event_id"):
            return raw_event

        event_with_id = dict(raw_event)
        event_with_id["event_id"] = str(uuid4())
        return event_with_id
