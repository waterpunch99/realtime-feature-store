from decimal import Decimal
from typing import Any

from app.repositories.feature_repository import FeatureRepository
from app.schemas.recommendation import RecommendationItem, RecommendationResponse
from app.services.feature_store import FeatureStoreService


class RecommendationService:
    def __init__(self, repository: FeatureRepository):
        self._repository = repository
        self._feature_store = FeatureStoreService(repository)

    async def recommend_for_user(self, user_id: str, *, limit: int) -> RecommendationResponse:
        user_features = await self._repository.get_feature_hash(f"feature:user:{user_id}") or {}
        recent_categories = self._recent_click_categories(user_features)
        excluded_product_ids = self._excluded_product_ids(user_features)

        candidates = await self._feature_store.get_popular_products(window="10m", limit=max(limit * 5, 20))
        ranked: list[RecommendationItem] = []

        for item in candidates:
            if item.id in excluded_product_ids:
                continue

            category_id = item.features.get("category_id")
            category_match = category_id in recent_categories if category_id else False
            boosted_score = item.score + (Decimal("1000000") if category_match else Decimal("0"))
            reason = "recent_click_category_popular" if category_match else "global_popular"

            ranked.append(
                RecommendationItem(
                    product_id=item.id,
                    score=boosted_score,
                    reason=reason,
                )
            )

        ranked.sort(key=lambda item: item.score, reverse=True)
        return RecommendationResponse(user_id=user_id, items=ranked[:limit])

    def _recent_click_categories(self, user_features: dict[str, Any]) -> set[str]:
        categories = (
            user_features.get("user_recent_click_categories_10m")
            or user_features.get("user_recent_click_categories_1h")
            or []
        )
        if isinstance(categories, str):
            return {categories}
        return {str(category_id) for category_id in categories}

    def _excluded_product_ids(self, user_features: dict[str, Any]) -> set[str]:
        excluded = set()
        last_product_id = user_features.get("user_last_product_id")
        if last_product_id:
            excluded.add(str(last_product_id))
        for key in ("user_recent_product_ids", "user_recent_purchase_product_ids"):
            values = user_features.get(key) or []
            if isinstance(values, str):
                excluded.add(values)
            else:
                excluded.update(str(value) for value in values)
        return excluded
