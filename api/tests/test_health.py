from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.health import router


class _HealthyDependency:
    async def healthcheck(self) -> bool:
        return True


class _Settings:
    app_name = "test-api"
    environment = "test"


def test_health_returns_ok_when_dependencies_are_healthy() -> None:
    app = FastAPI()
    app.state.database = _HealthyDependency()
    app.state.redis = _HealthyDependency()
    app.state.kafka = _HealthyDependency()
    app.state.settings = _Settings()
    app.include_router(router)

    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "test-api",
        "environment": "test",
        "dependencies": {
            "database": "ok",
            "redis": "ok",
            "kafka": "ok",
        },
    }

