from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EventType = Literal["view", "click", "add_to_cart", "purchase", "search"]


class EventProperties(BaseModel):
    model_config = ConfigDict(extra="allow")

    price: float | None = Field(default=None, ge=0)
    quantity: int | None = Field(default=None, ge=1)
    search_query: str | None = None
    page: str | None = None
    referrer: str | None = None


class UserEventBase(BaseModel):
    event_id: str | None = None
    event_type: EventType
    user_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    product_id: str | None = None
    category_id: str | None = None
    event_time: datetime
    ingest_time: datetime | None = None
    device_type: str | None = None
    country: str | None = None
    properties: EventProperties = Field(default_factory=EventProperties)


class EventEnvelope(BaseModel):
    event: UserEventBase
    metadata: dict[str, Any] = Field(default_factory=dict)


class EventIngestResponse(BaseModel):
    accepted: bool
    event_id: str
    topic: str
    partition: int | None = None
    offset: int | None = None


class BulkEventResult(BaseModel):
    index: int
    accepted: bool
    event_id: str | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    error_code: str | None = None
    error_message: str | None = None


class BulkEventIngestResponse(BaseModel):
    total: int
    accepted: int
    failed: int
    results: list[BulkEventResult]
