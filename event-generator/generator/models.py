from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EventType = Literal["view", "click", "add_to_cart", "purchase", "search"]


class EventProperties(BaseModel):
    model_config = ConfigDict(extra="allow")

    price: float | None = Field(default=None, ge=0)
    quantity: int | None = Field(default=None, ge=1)
    search_query: str | None = None
    page: str | None = None
    referrer: str | None = None


class SyntheticEvent(BaseModel):
    event_id: str | None = None
    event_type: EventType
    user_id: str
    session_id: str
    product_id: str | None = None
    category_id: str | None = None
    event_time: datetime
    device_type: str
    country: str
    properties: EventProperties = Field(default_factory=EventProperties)

    def to_payload(self) -> dict:
        return self.model_dump(mode="json", exclude_none=True)

