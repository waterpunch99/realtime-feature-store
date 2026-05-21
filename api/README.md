# API

FastAPI 기반 Event Collector, Feature Query, Recommendation API 모듈이다.

STEP 3 범위에서는 공통 설정, DB/Redis/Kafka 클라이언트 골격, structured logging, `GET /health`만 구현한다.

## Local Run

```bash
cd api
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

기본 로컬 인프라 접속값:

- PostgreSQL: `postgresql+asyncpg://feature_store:feature_store@localhost:15432/feature_store`
- Redis: `redis://localhost:16379/0`
- Kafka: `localhost:19092`

