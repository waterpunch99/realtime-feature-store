from generator.event_factory import EventFactory
from generator.scenarios import mixed_events, scenario_events


def test_scenario_events_pin_user_and_category() -> None:
    events = list(
        scenario_events(
            EventFactory(seed=1),
            5,
            user_id="u_10001",
            category_id="c_electronics",
        )
    )

    assert {event["user_id"] for event in events} == {"u_10001"}
    assert {event["category_id"] for event in events} == {"c_electronics"}


def test_mixed_events_can_emit_duplicates() -> None:
    events = list(
        mixed_events(
            EventFactory(seed=1),
            5,
            duplicate_ratio=1.0,
            invalid_ratio=0.0,
            late_ratio=0.0,
            seed=1,
        )
    )

    event_ids = [event["event_id"] for event in events]
    assert len(set(event_ids)) < len(event_ids)

