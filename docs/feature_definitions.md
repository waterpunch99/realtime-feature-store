# Feature Definitions

## Window 정책

| Window | Slide | MVP 구현 여부 |
| --- | --- | --- |
| 10분 | 1분 | 우선 구현 |
| 1시간 | 5분 | 우선 구현 |
| 24시간 | 30분 | 확장 대상 |

집계 기준 시간은 `event_time`이다.

## 사용자 피처

| 피처명 | 설명 |
| --- | --- |
| `user_recent_click_categories_10m` | 최근 10분 클릭 카테고리 목록 |
| `user_recent_click_categories_1h` | 최근 1시간 클릭 카테고리 목록 |
| `user_click_count_10m` | 최근 10분 클릭 수 |
| `user_click_count_1h` | 최근 1시간 클릭 수 |
| `user_view_count_10m` | 최근 10분 조회 수 |
| `user_add_to_cart_count_1h` | 최근 1시간 장바구니 수 |
| `user_purchase_count_1h` | 최근 1시간 구매 수 |
| `user_purchase_amount_1h` | 최근 1시간 구매 금액 합계 |
| `user_avg_purchase_amount_24h` | 최근 24시간 평균 구매 금액 |
| `user_last_event_time` | 마지막 이벤트 시각 |
| `user_last_product_id` | 마지막 상품 ID |

## 상품 피처

| 피처명 | 설명 |
| --- | --- |
| `product_view_count_10m` | 최근 10분 상품 조회 수 |
| `product_click_count_10m` | 최근 10분 상품 클릭 수 |
| `product_add_to_cart_count_10m` | 최근 10분 장바구니 수 |
| `product_purchase_count_1h` | 최근 1시간 구매 수 |
| `product_ctr_10m` | 최근 10분 클릭률 |
| `product_conversion_rate_1h` | 최근 1시간 구매 전환율 |
| `product_popularity_score_10m` | 최근 10분 인기도 점수 |

## 카테고리 피처

| 피처명 | 설명 |
| --- | --- |
| `category_view_count_10m` | 최근 10분 카테고리 조회 수 |
| `category_click_count_10m` | 최근 10분 카테고리 클릭 수 |
| `category_add_to_cart_count_1h` | 최근 1시간 장바구니 수 |
| `category_purchase_count_1h` | 최근 1시간 구매 수 |
| `category_purchase_amount_1h` | 최근 1시간 구매 금액 합계 |
| `category_popularity_score_10m` | 최근 10분 인기도 점수 |

## 인기도 점수 공식

```text
popularity_score = view_count * 1
                 + click_count * 3
                 + add_to_cart_count * 5
                 + purchase_count * 10
```

## 저장 키

Redis Hash:

- `feature:user:{user_id}`
- `feature:product:{product_id}`
- `feature:category:{category_id}`

Redis Sorted Set:

- `rank:product:popular:10m`
- `rank:category:popular:10m`

