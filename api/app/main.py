from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.config import Settings, get_settings
from app.database import Database
from app.kafka_client import KafkaProducerClient
from app.logging_config import configure_logging
from app.redis_client import RedisClient
from app.routers.events import router as events_router
from app.routers.features import router as features_router
from app.routers.health import router as health_router
from app.routers.recommendations import router as recommendations_router
from app.services.metrics_service import MetricsService


def create_lifespan(settings: Settings | None = None):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        effective_settings = settings or get_settings()
        configure_logging(effective_settings.log_level)

        app.state.settings = effective_settings
        app.state.database = Database(effective_settings)
        app.state.redis = RedisClient(effective_settings)
        app.state.kafka = KafkaProducerClient(effective_settings)
        app.state.metrics = MetricsService()

        try:
            yield
        finally:
            await app.state.kafka.close()
            await app.state.redis.close()
            await app.state.database.close()

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    application = FastAPI(
        title="Realtime Recommendation Feature Store API",
        version="0.1.0",
        lifespan=create_lifespan(settings),
    )

    @application.middleware("http")
    async def db_session_middleware(request: Request, call_next):
        database = getattr(request.app.state, "database", None)
        if database is None:
            return await call_next(request)

        async with database.session_factory() as session:
            request.state.db_session = session
            try:
                return await call_next(request)
            except Exception:
                await session.rollback()
                raise

    application.include_router(health_router)
    application.include_router(events_router)
    application.include_router(features_router)
    application.include_router(recommendations_router)
    return application


app = create_app()
