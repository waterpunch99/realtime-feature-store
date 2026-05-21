from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from app.schemas.event import UserEventBase


class EventValidationError(ValueError):
    def __init__(self, reason_code: str, reason_detail: str):
        self.reason_code = reason_code
        self.reason_detail = reason_detail
        super().__init__(reason_detail)


@dataclass(frozen=True)
class ValidatedEvent:
    event: UserEventBase
    kafka_key: str
    payload: dict[str, Any]


class EventValidationService:
    def validate_and_enrich(self, raw_event: dict[str, Any]) -> ValidatedEvent:
        try:
            event = UserEventBase.model_validate(raw_event)
        except ValidationError as exc:
            raise EventValidationError("schema_validation_failed", str(exc)) from exc

        if not event.event_id:
            event.event_id = str(uuid4())

        event.event_time = self._as_utc(event.event_time)
        event.ingest_time = datetime.now(UTC)

        self._validate_required_fields(event)
        self._validate_future_event_time(event)

        return ValidatedEvent(
            event=event,
            kafka_key=event.user_id,
            payload=event.model_dump(mode="json"),
        )

    def _validate_required_fields(self, event: UserEventBase) -> None:
        required_by_type = {
            "view": ["user_id", "session_id", "product_id", "category_id", "event_time"],
            "click": ["user_id", "session_id", "product_id", "category_id", "event_time"],
            "add_to_cart": [
                "user_id",
                "session_id",
                "product_id",
                "category_id",
                "event_time",
                "price",
                "quantity",
            ],
            "purchase": [
                "user_id",
                "session_id",
                "product_id",
                "category_id",
                "event_time",
                "price",
                "quantity",
            ],
            "search": ["user_id", "session_id", "event_time", "search_query"],
        }

        missing = []
        for field_name in required_by_type[event.event_type]:
            value = self._field_value(event, field_name)
            if self._is_missing(value):
                missing.append(field_name)

        if missing:
            raise EventValidationError(
                "required_field_missing",
                f"Missing required field(s) for {event.event_type}: {', '.join(missing)}",
            )

    def _validate_future_event_time(self, event: UserEventBase) -> None:
        now = datetime.now(UTC)
        if event.event_time > now + timedelta(minutes=10):
            raise EventValidationError(
                "event_time_too_far_in_future",
                "event_time is more than 10 minutes in the future",
            )

    def _field_value(self, event: UserEventBase, field_name: str) -> Any:
        if field_name in {"price", "quantity", "search_query"}:
            return getattr(event.properties, field_name)
        return getattr(event, field_name)

    def _is_missing(self, value: Any) -> bool:
        return value is None or (isinstance(value, str) and value.strip() == "")

    def _as_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
