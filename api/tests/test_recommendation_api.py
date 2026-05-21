from app.repositories.feature_repository import FeatureRepository
from app.services.recommendation_service import RecommendationService


class _Redis:
    def __init__(self) -> None:
        self.hashes = {
            "feature:user:u_1": {
                "user_recent_click_categories_10m": '["c_1"]',
                "user_last_product_id": '"p_excluded"',
            },
            "feature:product:p_1": {
                "category_id": '"c_1"',
                "product_popularity_score_10m": "10",
            },
            "feature:product:p_2": {
                "category_id": '"c_2"',
                "product_popularity_score_10m": "20",
            },
            "feature:product:p_excluded": {
                "category_id": '"c_1"',
                "product_popularity_score_10m": "999",
            },
        }
        self.ranks = {
            "rank:product:popular:10m": [
                ("p_excluded", 999.0),
                ("p_2", 20.0),
                ("p_1", 10.0),
            ]
        }

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def zrevrange(self, key, start, end, withscores=False):
        return self.ranks.get(key, [])[start : end + 1]


async def test_recommendation_excludes_last_product_and_boosts_recent_category() -> None:
    service = RecommendationService(FeatureRepository(_Redis()))

    response = await service.recommend_for_user("u_1", limit=2)

    assert [item.product_id for item in response.items] == ["p_1", "p_2"]
    assert response.items[0].reason == "recent_click_category_popular"

