# Step Plan

## STEP 0. 프로젝트 설계 문서와 전체 구조 생성

목표:

- 프로젝트 루트 구조를 만든다.
- README와 docs 문서를 작성한다.
- 비즈니스 로직 코드는 작성하지 않는다.

## STEP 1. Docker Compose 인프라 구성

목표:

- Kafka, PostgreSQL, Redis, Flink JobManager, Flink TaskManager를 Docker Compose로 구성한다.
- checkpoint/savepoint 경로를 사용할 수 있도록 구성한다.
- MVP 기본 State Backend는 `HashMapStateBackend`로 둔다.

## STEP 2. PostgreSQL 스키마 작성

목표:

- `db/init.sql`을 작성한다.
- quality log, late event log, feature latest/history 테이블을 만든다.
- `event_outbox` 테이블은 만들지 않는다.

## STEP 3. FastAPI 공통 설정, DB/Redis/Kafka 클라이언트 골격 구현

목표:

- FastAPI 앱 골격을 만든다.
- config, database, redis, kafka, logging 설정을 작성한다.
- `GET /health`를 구현한다.

## STEP 4. FastAPI Event Collector API와 Kafka publish 구현

목표:

- `POST /events`, `POST /events/bulk`를 구현한다.
- API가 `ingest_time`을 부여한다.
- 유효 이벤트를 Kafka `raw-user-events`로 publish한다.

## STEP 5. Python Synthetic Event Generator 구현

목표:

- Generator가 FastAPI Event Collector API로 HTTP 요청을 보낸다.
- normal, mixed, burst, scenario 모드를 제공한다.
- Kafka에 직접 publish하지 않는다.

## STEP 6. Kafka 토픽 생성 스크립트와 로컬 실행 스크립트 구현

목표:

- Kafka 토픽 생성 스크립트와 로컬 실행 스크립트를 작성한다.
- `run-outbox-relay.sh`는 작성하지 않는다.

## STEP 7. Java Flink Validation & Enrichment Job 구현

목표:

- `raw-user-events`를 소비한다.
- 유효 이벤트는 `clean-user-events`로 보낸다.
- invalid 이벤트는 `invalid-user-events-dlq`로 보낸다.

## STEP 8. Java Flink Feature Aggregation Job 구현

목표:

- `clean-user-events`를 소비한다.
- event_id dedup, TTL, event-time, watermark, sliding window aggregation을 구현한다.
- `ingest_time - event_time` 기준 late event는 `late-events-dlq`로 보낸다.

## STEP 9. Java Flink Redis/PostgreSQL Feature Sink 구현

목표:

- Redis Hash와 Sorted Set에 온라인 피처를 저장한다.
- PostgreSQL latest/history 테이블에 피처를 저장한다.

## STEP 10. FastAPI Feature Query & Recommendation API 구현

목표:

- 피처 조회 API, 인기 랭킹 API, rule-based recommendation API를 구현한다.

## STEP 11. 테스트, 데모 시나리오, README 완성

목표:

- Python pytest, Flink JUnit, Docker Compose E2E smoke test를 정리한다.
- README, 데모 절차, 트러블슈팅 문서를 완성한다.
