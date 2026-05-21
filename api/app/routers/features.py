from fastapi import APIRouter, HTTPException, Query, Request

from app.repositories.feature_repository import FeatureRepository
from app.schemas.feature import FeatureResponse, PopularResponse
from app.services.feature_store import FeatureNotFoundError, FeatureStoreService

router = APIRouter(tags=["features"])


@router.get("/features/users/{user_id}", response_model=FeatureResponse)
async def get_user_features(user_id: str, request: Request) -> FeatureResponse:
    request.app.state.metrics.increment("api_requests_total")
    return await _get_or_404(_service(request).get_user_features(user_id))


@router.get("/features/products/{product_id}", response_model=FeatureResponse)
async def get_product_features(product_id: str, request: Request) -> FeatureResponse:
    request.app.state.metrics.increment("api_requests_total")
    return await _get_or_404(_service(request).get_product_features(product_id))


@router.get("/features/categories/{category_id}", response_model=FeatureResponse)
async def get_category_features(category_id: str, request: Request) -> FeatureResponse:
    request.app.state.metrics.increment("api_requests_total")
    return await _get_or_404(_service(request).get_category_features(category_id))


@router.get("/popular-products", response_model=PopularResponse)
async def get_popular_products(
    request: Request,
    window: str = Query(default="10m"),
    limit: int = Query(default=20, ge=1, le=100),
) -> PopularResponse:
    request.app.state.metrics.increment("api_requests_total")
    try:
        items = await _service(request).get_popular_products(window=window, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PopularResponse(window=window, items=items)


@router.get("/popular-categories", response_model=PopularResponse)
async def get_popular_categories(
    request: Request,
    window: str = Query(default="10m"),
    limit: int = Query(default=20, ge=1, le=100),
) -> PopularResponse:
    request.app.state.metrics.increment("api_requests_total")
    try:
        items = await _service(request).get_popular_categories(window=window, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PopularResponse(window=window, items=items)


def _service(request: Request) -> FeatureStoreService:
    return FeatureStoreService(FeatureRepository(request.app.state.redis.client))


async def _get_or_404(awaitable):
    try:
        return await awaitable
    except FeatureNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
