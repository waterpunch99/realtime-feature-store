# realtime-recommendation-feature-store

이 프로젝트는 이커머스 사용자 행동 이벤트를 실시간으로 수집하고, Kafka와 Flink를 통해 추천 시스템용 피처를 계산한 뒤 Redis 온라인 피처 저장소와 PostgreSQL 피처 이력 저장소에 저장하는 포트폴리오용 실시간 피처 스토어입니다.

## 목표

- FastAPI Event Collector API가 사용자 행동 이벤트를 HTTP로 수신한다.
- Event Collector API는 수신 시점의 `ingest_time`을 부여하고 Kafka `raw-user-events` 토픽으로 publish한다.
- Synthetic Event Generator는 실제 클라이언트처럼 FastAPI API로 HTTP 요청을 보낸다.
- Java Flink DataStream Job이 이벤트 검증, 정제, 중복 제거, sliding window aggregation을 수행한다.
- Redis에는 최신 온라인 피처와 인기 랭킹을 저장한다.
- PostgreSQL에는 피처 최신 스냅샷, 피처 이력, 품질 로그, late event 로그를 저장한다.
- FastAPI Recommendation API는 피처 조회, 인기 상품 조회, rule-based 추천 결과를 제공한다.

## 핵심 아키텍처

```text
Python Synthetic Event Generator
  -> FastAPI Event Collector API
  -> Kafka raw-user-events
  -> Java Flink Validation & Enrichment Job
  -> Kafka clean-user-events or invalid-user-events-dlq
  -> Java Flink Feature Aggregation Job
  -> Redis Online Feature Store
  -> PostgreSQL Feature History Store
  -> FastAPI Recommendation API
```

## 중요한 설계 결정

- Synthetic Event Generator는 Kafka에 직접 이벤트를 보내지 않는다.
- Event Collector API가 Kafka producer를 소유한다.
- Event Collector API는 Kafka publish 성공을 확인한 뒤 성공 응답을 반환한다.
- 이 프로젝트는 Outbox Pattern을 사용하지 않는다.
- `event_outbox` 테이블과 Outbox Relay Worker는 구현하지 않는다.
- Flink Job은 Java 17과 Apache Flink Java DataStream API로만 구현한다.
- MVP 기본 Flink State Backend는 `HashMapStateBackend`이다.
- checkpoint interval 기본값은 60초이다.
- 로컬 checkpoint path는 `/tmp/flink-checkpoints`이다.
- 로컬 savepoint path는 `/tmp/flink-savepoints`이다.
- 대용량 상태 또는 운영 환경에서는 `EmbeddedRocksDBStateBackend`로 전환할 수 있도록 설정을 분리한다.

## MVP 범위

- 10분 sliding window, 1분 slide
- 1시간 sliding window, 5분 slide
- 24시간 window는 확장 가능하도록 설계하되 MVP에서는 우선순위를 낮춘다.
- 복잡한 Prometheus/Grafana는 제외하고 `/metrics` 엔드포인트로 기본 지표를 제공한다.
- ML 모델은 사용하지 않고 rule-based recommendation을 구현한다.

## 문서

- [Architecture](docs/architecture.md)
- [Event Schema](docs/event_schema.md)
- [Feature Definitions](docs/feature_definitions.md)
- [Processing Policy](docs/processing_policy.md)
- [API Contract](docs/api_contract.md)
- [Step Plan](docs/step_plan.md)
- [Testing](docs/testing.md)
- [Demo Scenario](docs/demo_scenario.md)
- [Troubleshooting](docs/troubleshooting.md)

## 로컬 실행 요약

인프라 기동과 Kafka 토픽 생성:

```bash
scripts/run-local.sh
```

API 실행:

```bash
cd api
python3 -m pip install -e ".[dev]"
../scripts/run-api.sh
```

Flink Job 빌드:

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

Flink Job 제출:

```bash
scripts/submit-flink-jobs.sh
```

이벤트 생성:

```bash
scripts/run-generator.sh --mode normal --rate 10 --duration 60
```

추천 API 조회:

```bash
curl "http://localhost:8000/popular-products?window=10m&limit=20"
curl "http://localhost:8000/recommendations/users/u_10001?limit=20"
```

## 주요 엔드포인트

```text
GET  /health
GET  /metrics
POST /events
POST /events/bulk
GET  /features/users/{user_id}
GET  /features/products/{product_id}
GET  /features/categories/{category_id}
GET  /popular-products?window=10m&limit=20
GET  /popular-categories?window=10m&limit=20
GET  /recommendations/users/{user_id}?limit=20
```

## 테스트

Python 테스트:

```bash
cd api
python3 -m pytest -q

cd ../event-generator
python3 -m pytest -q
```

Flink Java 테스트:

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle test --no-daemon
```

전체 테스트와 smoke test 절차는 [Testing](docs/testing.md)을 따른다.

## 처리 보장과 한계

이 프로젝트는 완전한 end-to-end exactly-once를 단정하지 않는다.

적용한 중복 방지 장치:

- FastAPI Kafka producer idempotence
- Kafka `acks=all`
- Flink checkpoint
- Flink `event_id` dedup state TTL
- Redis feature key 기준 overwrite
- PostgreSQL latest upsert
- PostgreSQL history unique constraint

Outbox Pattern을 사용하지 않기 때문에 API 수신과 Kafka publish를 하나의 DB transaction으로 묶지 않는다. API는 Kafka delivery 결과를 확인한 뒤 성공 응답을 반환하지만, API 프로세스 장애나 네트워크 장애 구간에서 운영 재처리 자동화는 제한적이다. 이 한계는 MVP 단순성과 포트폴리오 설명 가능성을 위한 선택이다.

## Flink State Backend 정책

MVP 기본 State Backend는 `HashMapStateBackend`이다.

- checkpoint interval: `60s`
- checkpoint path: `/tmp/flink-checkpoints`
- savepoint path: `/tmp/flink-savepoints`
- dedup state TTL: 기본 `25h`

HashMapStateBackend는 로컬 Docker Compose MVP에서 설정이 단순하고, 제한된 이벤트 볼륨의 데모에 적합하다. 운영 환경, 높은 key cardinality, 큰 dedup/window state, 장시간 window, 대규모 checkpoint 안정성이 필요해지면 `EmbeddedRocksDBStateBackend`로 전환한다.

## STEP 진행 규칙

이 저장소는 단계별로 구현한다. 사용자가 `STEP N 진행`이라고 요청하기 전까지 해당 STEP의 코드는 작성하지 않는다.

현재 완료 범위:

- STEP 0-11 완료
