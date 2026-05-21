# Event Schema

## 이벤트 타입

- `view`
- `click`
- `add_to_cart`
- `purchase`
- `search`

## 기본 이벤트 JSON

```json
{
  "event_id": "uuid",
  "event_type": "click",
  "user_id": "u_10001",
  "session_id": "s_90001",
  "product_id": "p_30001",
  "category_id": "c_electronics",
  "event_time": "2026-05-21T12:34:56.123Z",
  "ingest_time": null,
  "device_type": "mobile",
  "country": "KR",
  "properties": {
    "price": 129000,
    "quantity": 1,
    "search_query": null,
    "page": "product_detail",
    "referrer": "home"
  }
}
```

## 필드 정책

| 필드 | 설명 | 생성 주체 |
| --- | --- | --- |
| `event_id` | 이벤트 고유 ID. 없으면 API가 생성 | Generator 또는 API |
| `event_type` | 허용된 이벤트 타입 | Generator |
| `user_id` | 사용자 ID | Generator |
| `session_id` | 세션 ID | Generator |
| `product_id` | 상품 ID | Generator |
| `category_id` | 카테고리 ID | Generator |
| `event_time` | 실제 이벤트 발생 시각 | Generator |
| `ingest_time` | API 서버 수신 시각 | FastAPI Event Collector |
| `device_type` | 디바이스 유형 | Generator |
| `country` | 국가 코드 | Generator |
| `properties` | 이벤트별 확장 속성 | Generator |

Generator는 `ingest_time`을 확정하지 않는다. `ingest_time`은 FastAPI Event Collector API가 서버 수신 시점에 UTC 기준으로 부여한다.

## 이벤트별 필수값

| 이벤트 타입 | 필수값 |
| --- | --- |
| `view` | `user_id`, `session_id`, `product_id`, `category_id`, `event_time` |
| `click` | `user_id`, `session_id`, `product_id`, `category_id`, `event_time` |
| `add_to_cart` | `user_id`, `session_id`, `product_id`, `category_id`, `event_time`, `price`, `quantity` |
| `purchase` | `user_id`, `session_id`, `product_id`, `category_id`, `event_time`, `price`, `quantity` |
| `search` | `user_id`, `session_id`, `event_time`, `search_query` |

`price`, `quantity`, `search_query`는 `properties` 내부에 위치한다.

## 데이터 품질 규칙

- `event_id`는 null일 수 없다.
- `event_type`은 허용된 값이어야 한다.
- `user_id`는 null일 수 없다.
- `event_time`은 null일 수 없다.
- `price >= 0`
- `quantity >= 1`
- `purchase` 이벤트는 `product_id`, `category_id`, `price`, `quantity`가 필수이다.
- `add_to_cart` 이벤트는 `product_id`, `category_id`, `price`, `quantity`가 필수이다.
- `search` 이벤트는 `search_query`가 필수이다.
- 현재 시간보다 10분 이상 미래인 `event_time`은 invalid이다.
- 24시간 이상 과거인 `event_time`은 invalid 또는 late로 분류한다.

