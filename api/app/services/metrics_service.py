from dataclasses import dataclass
from typing import Literal


@dataclass
class MetricsSnapshot:
    api_requests_total: int = 0
    event_ingest_success_total: int = 0
    event_ingest_failure_total: int = 0
    kafka_publish_success_total: int = 0
    kafka_publish_failure_total: int = 0
    validation_failure_total: int = 0


class MetricsService:
    def __init__(self) -> None:
        self._snapshot = MetricsSnapshot()

    def snapshot(self) -> MetricsSnapshot:
        return self._snapshot

    def increment(self, metric: Literal[
        "api_requests_total",
        "event_ingest_success_total",
        "event_ingest_failure_total",
        "kafka_publish_success_total",
        "kafka_publish_failure_total",
        "validation_failure_total",
    ]) -> None:
        current = getattr(self._snapshot, metric)
        setattr(self._snapshot, metric, current + 1)
