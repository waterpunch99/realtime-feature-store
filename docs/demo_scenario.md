# Demo Scenario

## 목표

로컬에서 이벤트 생성부터 추천 API 조회까지 한 번에 설명 가능한 데모 흐름을 제공한다.

## 1. 인프라 시작

```bash
scripts/run-local.sh
```

확인:

```bash
docker compose ps
```

주요 포트:

- Kafka: `localhost:19092`
- PostgreSQL: `localhost:15432`
- Redis: `localhost:16379`
- Flink UI: `http://localhost:18081`

## 2. API 시작

```bash
scripts/run-api.sh
```

확인:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/metrics
```

## 3. Flink Job 빌드

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

## 4. Flink Job 제출

```bash
scripts/submit-flink-jobs.sh
```

Flink UI에서 `validation-enrichment-job`, `feature-aggregation-job` 실행 상태를 확인한다.

## 5. 시나리오 이벤트 생성

특정 사용자와 카테고리에 클릭/구매 패턴을 만든다.

```bash
scripts/run-generator.sh --mode scenario --user-id u_10001 --category-id c_electronics --rate 20 --duration 120
```

혼합 품질 이벤트도 생성할 수 있다.

```bash
scripts/run-generator.sh --mode mixed --rate 20 --duration 120 --duplicate-ratio 0.05 --invalid-ratio 0.02 --late-ratio 0.05
```

## 6. 저장소 확인

Redis:

```bash
docker compose exec -T redis redis-cli hgetall feature:user:u_10001
docker compose exec -T redis redis-cli zrevrange rank:product:popular:10m 0 20 withscores
docker compose exec -T redis redis-cli zrevrange rank:category:popular:10m 0 20 withscores
```

PostgreSQL:

```bash
docker compose exec -T postgres psql -U feature_store -d feature_store -c "SELECT * FROM feature_user_latest WHERE user_id = 'u_10001';"
docker compose exec -T postgres psql -U feature_store -d feature_store -c "SELECT count(*) FROM feature_product_history;"
docker compose exec -T postgres psql -U feature_store -d feature_store -c "SELECT count(*) FROM event_quality_log;"
```

## 7. API 조회

```bash
curl "http://localhost:8000/features/users/u_10001"
curl "http://localhost:8000/popular-products?window=10m&limit=20"
curl "http://localhost:8000/popular-categories?window=10m&limit=20"
curl "http://localhost:8000/recommendations/users/u_10001?limit=20"
```

## 발표 포인트

- Generator는 Kafka에 직접 접근하지 않고 HTTP로 API에 이벤트를 보낸다.
- API는 `ingest_time`을 부여하고 Kafka delivery 결과를 확인한다.
- Flink Validation Job이 invalid 이벤트를 DLQ로 분리한다.
- Flink Aggregation Job이 event-time, watermark, dedup TTL, sliding window aggregation을 사용한다.
- Redis는 온라인 피처 조회와 인기 랭킹을 담당한다.
- PostgreSQL은 latest snapshot과 history를 담당한다.
- Outbox Pattern을 쓰지 않는 구조적 한계를 문서화했다.

