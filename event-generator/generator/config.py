from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    collector_url: str = "http://localhost:8000/events"
    request_timeout_seconds: float = 5.0
    default_rate: int = 10
    default_duration_seconds: int = 60

