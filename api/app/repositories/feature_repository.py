import json
from decimal import Decimal
from typing import Any

from redis.asyncio import Redis


class FeatureRepository:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def get_feature_hash(self, key: str) -> dict[str, Any] | None:
        raw_hash = await self._redis.hgetall(key)
        if not raw_hash:
            return None
        return {field: self._decode_value(value) for field, value in raw_hash.items()}

    async def get_ranked_items(self, key: str, limit: int) -> list[tuple[str, Decimal]]:
        raw_items = await self._redis.zrevrange(key, 0, max(limit - 1, 0), withscores=True)
        return [(str(item_id), Decimal(str(score))) for item_id, score in raw_items]

    async def get_product_features(self, product_id: str) -> dict[str, Any]:
        return await self.get_feature_hash(f"feature:product:{product_id}") or {}

    async def get_category_features(self, category_id: str) -> dict[str, Any]:
        return await self.get_feature_hash(f"feature:category:{category_id}") or {}

    def _decode_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
