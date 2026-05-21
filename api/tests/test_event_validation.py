from datetime import UTC, datetime, timedelta

import pytest

from app.services.event_validation import EventValidationError, EventValidationService


def _base_event() -> dict:
    return {
        "event_id": "e_1",
        "event_type": "click",
        "user_id": "u_1",
        "session_id": "s_1",
        "product_id": "p_1",
        "category_id": "c_1",
        "event_time": datetime.now(UTC).isoformat(),
        "properties": {},
    }


def test_validate_and_enrich_assigns_ingest_time() -> None:
    validated = EventValidationService().validate_and_enrich(_base_event())

    assert validated.kafka_key == "u_1"
    assert validated.payload["event_id"] == "e_1"
    assert validated.payload["ingest_time"] is not None


def test_add_to_cart_requires_price_and_quantity() -> None:
    event = _base_event()
    event["event_type"] = "add_to_cart"

    with pytest.raises(EventValidationError) as exc_info:
        EventValidationService().validate_and_enrich(event)

    assert exc_info.value.reason_code == "required_field_missing"
    assert "price" in exc_info.value.reason_detail
    assert "quantity" in exc_info.value.reason_detail


def test_future_event_more_than_ten_minutes_is_invalid() -> None:
    event = _base_event()
    event["event_time"] = (datetime.now(UTC) + timedelta(minutes=11)).isoformat()

    with pytest.raises(EventValidationError) as exc_info:
        EventValidationService().validate_and_enrich(event)

    assert exc_info.value.reason_code == "event_time_too_far_in_future"

