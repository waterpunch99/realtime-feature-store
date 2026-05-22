# Baseline Experiment Result

## 목적

정상 이벤트만 입력했을 때 로컬 실시간 Feature Store 파이프라인이 end-to-end로 안정 동작하는지 확인한다.

이 실험은 이후 throughput, duplicate ratio, late ratio 실험의 비교 기준선이다.

## 실행 환경

- 실행일: 2026-05-22
- Git commit: `d4416e5`
- 인프라: Docker Compose
- Kafka: `confluentinc/cp-kafka:7.6.1`
- Flink: `flink:1.19-java17`
- PostgreSQL: `postgres:16`
- Redis: `redis:7`
- API 실행: `python:3.11-slim` 컨테이너
- Generator 실행: `python:3.11-slim` 컨테이너

## 입력 조건

```text
mode: normal
rate: 100 eps
duration: 120s
duplicate_ratio: 0
invalid_ratio: 0
late_ratio: 0
seed: 20260522
target_events: 12000
```

Generator 실행:

```bash
docker run --rm \
  --network container:rfs-api-baseline \
  -v "$PWD/event-generator:/workspace/event-generator" \
  -w /workspace/event-generator \
  python:3.11-slim \
  sh -c 'python -m pip install -q -e ".[dev]" && \
    python -m generator.main \
      --collector-url http://127.0.0.1:8000/events \
      --mode normal \
      --rate 100 \
      --duration 120 \
      --seed 20260522'
```

Generator 결과:

```text
completed sent=12000 accepted=12000 failed=0
```

## API Metrics

```json
{
  "api_requests_total": 12001,
  "event_ingest_success_total": 12000,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 12000,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

해석:

- 정상 이벤트 12,000건이 모두 API validation과 Kafka publish를 통과했다.
- 실패율은 0%다.
- `api_requests_total`은 health/metrics 조회 1건을 포함해 입력 이벤트 수보다 1 크다.

## Flink Job 상태

```text
validation-enrichment-job RUNNING
feature-aggregation-job   RUNNING
```

Job ID:

```text
validation-enrichment-job 517cfe1c9a93f012dcb9872b21252dc0
feature-aggregation-job   5aac548ca4d197de0faeae9bd9dde937
```

비고:

- 최초 job 제출 시 checkpoint directory 권한 문제로 validation job 1건이 실패했다.
- `/tmp/flink-checkpoints`, `/tmp/flink-savepoints` 권한을 조정한 뒤 재제출한 job은 정상 실행됐다.

## Kafka Consumer Lag

Validation job:

```text
GROUP                     TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
validation-enrichment-job raw-user-events 0          3979            3979            0
validation-enrichment-job raw-user-events 1          3990            3990            0
validation-enrichment-job raw-user-events 2          4031            4031            0
```

Aggregation job:

```text
GROUP                   TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
feature-aggregation-job clean-user-events 0          4149            4149            0
feature-aggregation-job clean-user-events 1          3938            3938            0
feature-aggregation-job clean-user-events 2          3913            3913            0
```

해석:

- raw topic과 clean topic 모두 최종 lag가 0으로 수렴했다.
- 정상 이벤트 12,000건은 validation과 aggregation consumer가 모두 따라잡았다.

## DLQ Topic

```text
invalid-user-events-dlq:0:0
invalid-user-events-dlq:1:0
invalid-user-events-dlq:2:0

late-events-dlq:0:0
late-events-dlq:1:0
late-events-dlq:2:0
```

해석:

- 정상 이벤트 기준선에서는 invalid DLQ와 late DLQ 모두 0건이다.
- 이후 invalid ratio, late ratio 실험에서 DLQ 증가율을 비교할 기준값으로 사용한다.

## Feature Update Topic

```text
feature-user-updates:0:3483
feature-user-updates:1:3258
feature-user-updates:2:3363

feature-product-updates:0:184
feature-product-updates:1:231
feature-product-updates:2:164

feature-category-updates:0:0
feature-category-updates:1:16
feature-category-updates:2:8
```

합계:

```text
feature-user-updates: 10104
feature-product-updates: 579
feature-category-updates: 24
```

## Redis 결과

Redis key count:

```text
4273
```

인기 상품 랭킹 샘플:

```text
p_00083  107
p_00117   98
p_00057   95
p_00153   92
p_00035   90
```

## PostgreSQL 결과

```text
event_quality_log | user_history | product_history | category_history
------------------+--------------+-----------------+-----------------
0                 | 10104        | 579             | 24
```

샘플 사용자 latest feature:

```text
user_id  | user_click_count_10m | user_view_count_10m | updated_at
---------+----------------------+---------------------+-------------------------------
u_18349  | 1                    | 0                   | 2026-05-22 10:27:43.006939+00
u_15202  | 1                    | 0                   | 2026-05-22 10:27:43.00692+00
u_14278  | 1                    | 0                   | 2026-05-22 10:27:43.0069+00
```

## Flink Checkpoint

Aggregation job checkpoint summary:

```text
completed checkpoints: 9
failed checkpoints: 0
latest checkpoint size: 26,815,705 bytes
latest checkpoint duration: 291 ms
p95 checkpoint duration: 48,573 ms
p95 checkpointed size: 26,815,705 bytes
alignment buffered: 0
```

해석:

- checkpoint 실패는 없었다.
- 일부 checkpoint duration이 길게 튀었다. 특히 checkpoint 7은 48.573초가 걸렸다.
- throughput/checkpoint 실험에서는 checkpoint duration outlier를 별도로 관찰해야 한다.

## 판정

기준선 실험은 통과로 본다.

- API 수집 성공률: 100%
- Kafka publish failure: 0
- validation failure: 0
- invalid DLQ: 0
- late DLQ: 0
- raw topic final lag: 0
- clean topic final lag: 0
- Redis/PostgreSQL feature 생성: 정상
- Flink job 상태: RUNNING
- Aggregation checkpoint 실패: 0

## 관찰된 제약

Generator는 목표 입력 조건을 `100 eps, 120s`로 설정했지만, 실제 실행 중 목표 rate보다 낮은 체감 처리 속도를 보였다.

원인 후보:

- `EventSender`가 이벤트마다 `httpx.AsyncClient`를 새로 생성한다.
- generator가 각 HTTP 요청 로그를 매우 많이 출력한다.
- generator가 단일 coroutine에서 순차 전송한다.

따라서 다음 throughput 실험 전에 generator를 부하 생성 도구로 쓰려면 아래 개선이 필요하다.

- `httpx.AsyncClient` 재사용
- per-request HTTP 로그 억제
- 동시성 또는 batch 전송 옵션 추가
- 실제 wall-clock duration과 achieved eps 출력

현재 기준선 실험은 "정상 이벤트에서 파이프라인이 정확히 처리되는가"를 확인하는 데 사용하고, "100 eps를 안정 처리했는가"라는 성능 결론에는 사용하지 않는다.
