from datetime import UTC, datetime

from generator.event_factory import EventFactory


def test_normal_event_has_event_time_but_no_ingest_time() -> None:
    event = EventFactory(seed=1).normal_event(event_type="click")
    payload = event.to_payload()

    assert payload["event_type"] == "click"
    assert payload["event_time"]
    assert "ingest_time" not in payload


def test_purchase_event_contains_price_and_quantity() -> None:
    payload = EventFactory(seed=1).normal_event(event_type="purchase").to_payload()

    assert payload["product_id"]
    assert payload["category_id"]
    assert payload["properties"]["price"] >= 0
    assert payload["properties"]["quantity"] >= 1


def test_invalid_event_can_generate_invalid_payload() -> None:
    payload = EventFactory(seed=3).invalid_event()

    assert "event_type" in payload
    assert payload["event_time"]


def test_duplicate_pair_uses_same_event_id() -> None:
    first, second = EventFactory(seed=1).duplicate_pair()

    assert first["event_id"] == second["event_id"]
    assert first == second


def test_late_event_uses_past_event_time() -> None:
    payload = EventFactory(seed=1).late_event(minutes_late=10)

    event_time = datetime.fromisoformat(payload["event_time"])
    assert event_time < datetime.now(UTC)

