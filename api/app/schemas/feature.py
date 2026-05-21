from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


class FeatureResponse(BaseModel):
    entity_id: str
    entity_type: str
    features: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime | None = None


class PopularItem(BaseModel):
    id: str
    score: Decimal
    features: dict[str, Any] = Field(default_factory=dict)


class PopularResponse(BaseModel):
    window: str
    items: list[PopularItem]
