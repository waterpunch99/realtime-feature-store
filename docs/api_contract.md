# API Contract

## 공통

Base URL:

```text
http://localhost:8000
```

Response format은 STEP 구현 과정에서 Pydantic schema로 확정한다. 이 문서는 STEP 0 기준의 계약 초안이다.

## Health and Metrics

### `GET /health`

API 프로세스 상태를 반환한다.

### `GET /metrics`

MVP 기본 지표를 반환한다.

포함 대상:

- API 요청 수
- 이벤트 수집 성공 수
- 이벤트 수집 실패 수
- Kafka publish 성공 수
- Kafka publish 실패 수
- validation 실패 수

## Event Collector API

### `POST /events`

단일 이벤트를 수신한다.

정책:

- `event_id`가 없으면 API가 생성한다.
- `ingest_time`은 API 서버 수신 시점의 UTC 시간으로 부여한다.
- 기본 스키마와 이벤트별 필수값을 검증한다.
- 유효 이벤트는 Kafka `raw-user-events`로 publish한다.
- Kafka key는 `user_id`이다.
- publish 성공 확인 후 성공 응답을 반환한다.
- 명백한 invalid 이벤트는 `event_quality_log`에 기록하고 400 응답을 반환한다.

### `POST /events/bulk`

여러 이벤트를 수신한다.

정책:

- 각 이벤트별 성공/실패 결과를 반환한다.
- 유효한 이벤트만 Kafka로 publish한다.
- 실패 이벤트는 실패 사유를 포함한다.

## Feature API

### `GET /features/users/{user_id}`

사용자 온라인 피처를 조회한다.

### `GET /features/products/{product_id}`

상품 온라인 피처를 조회한다.

### `GET /features/categories/{category_id}`

카테고리 온라인 피처를 조회한다.

## Popular API

### `GET /popular-products?window=10m&limit=20`

인기 상품 랭킹을 조회한다.

### `GET /popular-categories?window=10m&limit=20`

인기 카테고리 랭킹을 조회한다.

## Recommendation API

### `GET /recommendations/users/{user_id}?limit=20`

rule-based 추천 결과를 반환한다.

정책:

- 사용자의 최근 클릭 카테고리를 조회한다.
- 해당 카테고리 인기 상품과 전체 인기 상품을 섞는다.
- 사용자가 최근 클릭하거나 구매한 상품은 제외한다.
- `popularity_score` 기준으로 정렬한다.
- 상위 N개를 반환한다.

