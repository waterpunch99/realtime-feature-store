from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "realtime-recommendation-feature-store-api"
    environment: Literal["local", "test", "prod"] = "local"
    log_level: str = "INFO"

    database_url: str = (
        "postgresql+asyncpg://feature_store:feature_store@localhost:15432/feature_store"
    )
    redis_url: str = "redis://localhost:16379/0"

    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_client_id: str = "feature-store-api"
    kafka_raw_user_events_topic: str = "raw-user-events"
    kafka_request_timeout_seconds: float = Field(default=10.0, gt=0)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="RFS_",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()

