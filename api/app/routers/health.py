from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
    database_ok = await request.app.state.database.healthcheck()
    redis_ok = await request.app.state.redis.healthcheck()
    kafka_ok = await request.app.state.kafka.healthcheck()

    dependencies = {
        "database": "ok" if database_ok else "error",
        "redis": "ok" if redis_ok else "error",
        "kafka": "ok" if kafka_ok else "error",
    }

    return {
        "status": "ok" if all(value == "ok" for value in dependencies.values()) else "degraded",
        "service": request.app.state.settings.app_name,
        "environment": request.app.state.settings.environment,
        "dependencies": dependencies,
    }


@router.get("/metrics")
async def metrics(request: Request) -> dict[str, int]:
    request.app.state.metrics.increment("api_requests_total")
    return request.app.state.metrics.snapshot().__dict__
