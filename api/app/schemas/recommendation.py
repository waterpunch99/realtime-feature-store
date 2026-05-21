from decimal import Decimal

from pydantic import BaseModel


class RecommendationItem(BaseModel):
    product_id: str
    score: Decimal
    reason: str


class RecommendationResponse(BaseModel):
    user_id: str
    items: list[RecommendationItem]

