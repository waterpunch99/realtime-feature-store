from app.repositories.feature_repository import FeatureRepository
from app.services.feature_store import FeatureStoreService


class _Redis:
    def __init__(self) -> None:
        self.hashes = {
            "feature:user:u_1": {
                "user_click_count_10m": "3",
                "user_recent_click_categories_10m": '["c_1"]',
                "user_last_product_id": '"p_9"',
            },
            "feature:product:p_1": {
                "product_popularity_score_10m": "11",
                "category_id": '"c_1"',
            },
            "feature:category:c_1": {
                "category_popularity_score_10m": "7",
            },
        }
        self.ranks = {
            "rank:product:popular:10m": [("p_1", 11.0)],
            "rank:category:popular:10m": [("c_1", 7.0)],
        }

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def zrevrange(self, key, start, end, withscores=False):
        return self.ranks.get(key, [])[start : end + 1]


async def test_get_user_features_decodes_redis_hash_values() -> None:
    service = FeatureStoreService(FeatureRepository(_Redis()))

    response = await service.get_user_features("u_1")

    assert response.entity_id == "u_1"
    assert response.features["user_click_count_10m"] == 3
    assert response.features["user_recent_click_categories_10m"] == ["c_1"]


async def test_get_popular_products_reads_rank_and_features() -> None:
    service = FeatureStoreService(FeatureRepository(_Redis()))

    response = await service.get_popular_products(window="10m", limit=10)

    assert response[0].id == "p_1"
    assert response[0].features["category_id"] == "c_1"

