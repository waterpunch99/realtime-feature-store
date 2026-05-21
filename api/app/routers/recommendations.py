from fastapi import APIRouter, Query, Request

from app.repositories.feature_repository import FeatureRepository
from app.schemas.recommendation import RecommendationResponse
from app.services.recommendation_service import RecommendationService

router = APIRouter(tags=["recommendations"])


@router.get("/recommendations/users/{user_id}", response_model=RecommendationResponse)
async def recommend_for_user(
    user_id: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
) -> RecommendationResponse:
    request.app.state.metrics.increment("api_requests_total")
    service = RecommendationService(FeatureRepository(request.app.state.redis.client))
    return await service.recommend_for_user(user_id, limit=limit)
