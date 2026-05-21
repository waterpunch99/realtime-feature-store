# Architecture

## 개요

`realtime-recommendation-feature-store`는 실시간 추천 피처를 계산하기 위한 로컬 Docker Compose 기반 MVP 프로젝트이다. 이벤트 수집, 스트리밍 처리, 온라인 피처 저장, 피처 이력 저장, 추천 API를 분리해 데이터 플랫폼의 주요 책임 경계를 보여준다.

## 데이터 흐름

```text
Generator
  -> FastAPI /events, /events/bulk
  -> Kafka raw-user-events
  -> Flink Validation & Enrichment Job
  -> Kafka clean-user-events
  -> Flink Feature Aggregation Job
  -> Redis + PostgreSQL
  -> FastAPI Feature and Recommendation API
```

Invalid 또는 late 이벤트는 별도 경로로 분리한다.

```text
Validation failure -> Kafka invalid-user-events-dlq
Late event         -> Kafka late-events-dlq + PostgreSQL late_event_log
```

## 컴포넌트 책임

### Synthetic Event Generator

- Faker 기반으로 이벤트를 생성한다.
- 실제 앱/클라이언트처럼 FastAPI Event Collector API에 HTTP 요청을 보낸다.
- Kafka producer를 갖지 않는다.
- `event_time`은 생성하지만 `ingest_time`은 확정하지 않는다.

### FastAPI Event Collector API

- `POST /events`, `POST /events/bulk`를 제공한다.
- 수신 이벤트에 `event_id`가 없으면 생성한다.
- 서버 수신 시점의 UTC 시각을 `ingest_time`으로 부여한다.
- 기본 스키마와 이벤트별 필수값을 검증한다.
- 유효한 이벤트를 Kafka `raw-user-events`로 publish한다.
- Kafka key는 `user_id`를 사용한다.
- Kafka publish 실패 시 오류 응답과 structured logging을 남긴다.

### Kafka

Kafka는 이벤트와 피처 업데이트의 스트리밍 버퍼이다.

토픽:

- `raw-user-events`
- `clean-user-events`
- `invalid-user-events-dlq`
- `late-events-dlq`
- `feature-user-updates`
- `feature-product-updates`
- `feature-category-updates`

### Flink Validation & Enrichment Job

- `raw-user-events`를 소비한다.
- JSON을 Java 이벤트 모델로 파싱한다.
- 스키마와 데이터 품질을 검증한다.
- 유효 이벤트는 `clean-user-events`로 보낸다.
- invalid 이벤트는 `invalid-user-events-dlq`로 보낸다.
- event-time과 watermark를 설정한다.

### Flink Feature Aggregation Job

- `clean-user-events`를 소비한다.
- `event_id` 기준 stateful dedup을 수행한다.
- dedup state TTL을 적용한다.
- event-time processing, watermark, sliding window aggregation을 사용한다.
- 사용자, 상품, 카테고리 피처를 계산한다.
- 2분 초과 late event는 `late-events-dlq`로 보낸다.

### Redis Online Feature Store

- 추천 API에서 빠르게 읽을 최신 피처를 저장한다.
- Hash key:
  - `feature:user:{user_id}`
  - `feature:product:{product_id}`
  - `feature:category:{category_id}`
- Sorted Set key:
  - `rank:product:popular:10m`
  - `rank:category:popular:10m`

### PostgreSQL Feature History Store

- 피처 latest snapshot을 upsert한다.
- 피처 history를 window 단위로 insert한다.
- event quality log와 late event log를 저장한다.
- Outbox 저장소로 사용하지 않는다.

### FastAPI Feature and Recommendation API

- Redis에서 온라인 피처를 조회한다.
- 인기 상품과 인기 카테고리를 조회한다.
- rule-based recommendation을 제공한다.
- ML 모델은 사용하지 않는다.

## Outbox Pattern 미사용

이 프로젝트는 Outbox Pattern을 사용하지 않는다. 따라서 Event Collector API가 이벤트를 수신한 뒤 Kafka publish에 실패할 수 있는 장애 지점이 존재한다.

MVP에서는 다음 방식으로 한계를 관리한다.

- Kafka publish 성공 확인 후 API 성공 응답 반환
- publish 실패 시 오류 응답 반환
- structured logging으로 실패 원인 기록
- metrics로 publish 성공/실패 카운트 노출

이 구조는 포트폴리오 MVP에서 단순성과 실시간 수집 경로의 명확성을 우선한 선택이다.

