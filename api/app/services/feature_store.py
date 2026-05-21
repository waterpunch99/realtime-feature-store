from decimal import Decimal
from typing import Any

from app.repositories.feature_repository import FeatureRepository
from app.schemas.feature import FeatureResponse, PopularItem


class FeatureNotFoundError(LookupError):
    pass


class FeatureStoreService:
    def __init__(self, repository: FeatureRepository):
        self._repository = repository

    async def get_user_features(self, user_id: str) -> FeatureResponse:
        return await self._get_features("user", user_id, f"feature:user:{user_id}")

    async def get_product_features(self, product_id: str) -> FeatureResponse:
        return await self._get_features("product", product_id, f"feature:product:{product_id}")

    async def get_category_features(self, category_id: str) -> FeatureResponse:
        return await self._get_features("category", category_id, f"feature:category:{category_id}")

    async def get_popular_products(self, *, window: str, limit: int) -> list[PopularItem]:
        self._validate_window(window)
        ranked_items = await self._repository.get_ranked_items(
            f"rank:product:popular:{window}",
            limit,
        )
        return [
            PopularItem(
                id=product_id,
                score=score,
                features=await self._repository.get_product_features(product_id),
            )
            for product_id, score in ranked_items
        ]

    async def get_popular_categories(self, *, window: str, limit: int) -> list[PopularItem]:
        self._validate_window(window)
        ranked_items = await self._repository.get_ranked_items(
            f"rank:category:popular:{window}",
            limit,
        )
        return [
            PopularItem(
                id=category_id,
                score=score,
                features=await self._repository.get_category_features(category_id),
            )
            for category_id, score in ranked_items
        ]

    async def _get_features(self, entity_type: str, entity_id: str, key: str) -> FeatureResponse:
        features = await self._repository.get_feature_hash(key)
        if features is None:
            raise FeatureNotFoundError(f"{entity_type} feature not found: {entity_id}")

        updated_at = features.get("updated_at")
        return FeatureResponse(
            entity_id=entity_id,
            entity_type=entity_type,
            features=features,
            updated_at=updated_at if not isinstance(updated_at, str) else None,
        )

    def _validate_window(self, window: str) -> None:
        if window != "10m":
            raise ValueError("Only window=10m is supported for popularity ranking in MVP")


def score_value(features: dict[str, Any], key: str) -> Decimal:
    value = features.get(key)
    if value is None:
        return Decimal("0")
    return Decimal(str(value))
