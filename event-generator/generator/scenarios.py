from collections.abc import Iterator
from random import Random

from generator.event_factory import EventFactory


def normal_events(factory: EventFactory, total: int) -> Iterator[dict]:
    for _ in range(total):
        yield factory.normal_event().to_payload()


def burst_events(factory: EventFactory, total: int) -> Iterator[dict]:
    for _ in range(total):
        yield factory.normal_event().to_payload()


def scenario_events(
    factory: EventFactory,
    total: int,
    *,
    user_id: str,
    category_id: str,
) -> Iterator[dict]:
    for _ in range(total):
        yield factory.scenario_event(user_id=user_id, category_id=category_id).to_payload()


def mixed_events(
    factory: EventFactory,
    total: int,
    *,
    duplicate_ratio: float,
    invalid_ratio: float,
    late_ratio: float,
    seed: int | None = None,
) -> Iterator[dict]:
    random = Random(seed)
    last_valid_event: dict | None = None

    for _ in range(total):
        draw = random.random()
        if draw < duplicate_ratio:
            if last_valid_event is None:
                last_valid_event = factory.normal_event().to_payload()
            yield dict(last_valid_event)
            continue

        if draw < duplicate_ratio + invalid_ratio:
            yield factory.invalid_event()
            continue

        if draw < duplicate_ratio + invalid_ratio + late_ratio:
            payload = factory.late_event()
            last_valid_event = payload
            yield payload
            continue

        payload = factory.normal_event().to_payload()
        last_valid_event = payload
        yield payload
