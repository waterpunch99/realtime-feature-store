# Throughput Experiment Result

## 목적

정상 이벤트만 입력했을 때 입력 rate 증가가 API 수집, Kafka lag, Flink aggregation, Redis/PostgreSQL feature update에 미치는 영향을 확인한다.

이 실험은 duplicate/late ratio 실험 전에 "품질 이슈가 없는 상태의 처리 기준선"을 잡기 위한 것이다.

## 실행 환경

- 실행일: 2026-05-22
- 인프라: Docker Compose
- Kafka: `confluentinc/cp-kafka:7.6.1`
- Flink: `flink:1.19-java17`
- PostgreSQL: `postgres:16`
- Redis: `redis:7`
- API 실행: `python:3.11-slim` 컨테이너
- Generator 실행: `python:3.11-slim` 컨테이너
- Flink parallelism: 1
- Kafka partitions: 3
- Checkpoint interval: 60s

## 사전 수정

기준선 실험에서 generator가 실제 목표 rate를 만들지 못하는 문제가 보여, throughput 실험 전에 generator를 최소 보정했다.

변경 사항:

- `httpx.AsyncClient`를 이벤트마다 새로 만들지 않고 run 전체에서 재사용
- `httpx` per-request 로그 억제
- 완료 로그에 `elapsed_seconds`, `achieved_rate` 출력

검증:

```text
event-generator tests: 8 passed
```

## Run 조건

| Run | target rate | duration | target events | mode |
| --- | ---: | ---: | ---: | --- |
| T1 | 10 eps | 120s | 1,200 | normal |
| T2 | 100 eps | 120s | 12,000 | normal |
| T3 | 500 eps | 120s | 60,000 | normal |

T3는 generator가 목표 500 eps를 만들지 못해 중간에 중단했다. 따라서 T3는 "500 eps 처리 결과"가 아니라 "현재 단일 순차 generator의 부하 생성 한계"로 해석한다.

## Generator 결과

| Run | target eps | sent | accepted | failed | elapsed seconds | achieved eps | 판정 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| T1 | 10 | 1,200 | 1,200 | 0 | 121.566 | 9.87 | 유효 |
| T2 | 100 | 12,000 | 12,000 | 0 | 133.585 | 89.83 | 유효 |
| T3 | 500 | 약 12,070 | 약 12,070 | 0 | 중단 | 약 100 | 미성립 |

해석:

- T1은 목표 10 eps를 거의 정확히 만들었다.
- T2는 실패 없이 처리됐지만 실제 부하는 100 eps가 아니라 약 90 eps다.
- T3는 단일 순차 HTTP generator의 한계로 목표 500 eps를 만들지 못했다.

## API Metrics

T1 이후:

```json
{
  "api_requests_total": 1201,
  "event_ingest_success_total": 1200,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 1200,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

T2 이후 누적:

```json
{
  "api_requests_total": 13202,
  "event_ingest_success_total": 13200,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 13200,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

T3 중단 이후 누적:

```json
{
  "api_requests_total": 25273,
  "event_ingest_success_total": 25270,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 25270,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

해석:

- T1, T2, T3 partial 모두 API validation failure와 Kafka publish failure는 0이다.
- API 기준 누적 정상 수집 이벤트는 25,270건이다.

## Kafka Consumer Lag

### T1 이후

Validation job:

```text
raw-user-events partition offsets: 399, 394, 407
final lag: 0
```

Aggregation job:

```text
clean-user-events partition offsets: 414, 403, 383
final lag: 0
```

### T2 이후

Validation job:

```text
raw-user-events partition offsets: 4378, 4384, 4438
final lag: 0
```

Aggregation job:

```text
clean-user-events partition offsets: 4563, 4341, 4296
final lag: 0
```

### T3 중단 직후

Validation job:

```text
raw-user-events partition offsets: 8380, 8395, 8495
final lag: 0
```

Aggregation job은 중단 직후 일시적으로 lag가 남았다.

```text
clean-user-events lag: 4176, 3961, 3933
total lag: 12070
```

약 30초 이상 대기 후 aggregation lag는 0으로 수렴했다.

```text
clean-user-events partition offsets: 8739, 8302, 8229
final lag: 0
```

해석:

- validation job은 모든 run에서 빠르게 따라잡았다.
- aggregation job은 T3 partial 이후 checkpoint와 함께 일시적으로 밀렸지만 최종적으로 lag 0으로 수렴했다.
- peak lag를 정밀 측정하지는 못했으므로, 다음 성능 실험에서는 run 중 주기적 lag sampling이 필요하다.

## Flink Checkpoint

T1 이후 aggregation checkpoint:

```text
completed checkpoints: 18
failed checkpoints: 0
p95 checkpoint duration: 209 ms
p95 checkpointed size: 4,069,583 bytes
```

T2 이후 aggregation checkpoint:

```text
completed checkpoints: 24
failed checkpoints: 0
p95 checkpoint duration: 37,272.75 ms
p99 checkpoint duration: 49,556 ms
p95 checkpointed size: 27,943,753 bytes
```

T3 partial 이후 aggregation checkpoint:

```text
completed checkpoints: 37
failed checkpoints: 0
p95 checkpoint duration: 177,105.10 ms
p99 checkpoint duration: 375,196 ms
p95 checkpointed size: 29,817,554 bytes
```

해석:

- checkpoint 실패는 없었다.
- T2부터 checkpoint duration outlier가 크게 발생했다.
- T3 partial 이후에는 aggregation lag가 일시적으로 쌓였고, checkpoint duration p95/p99가 크게 튀었다.
- 이 결과는 별도의 checkpoint interval 실험에서 더 깊게 봐야 한다.

## Redis/PostgreSQL 결과

T3 partial까지 완료 후:

```text
Redis key count: 9184
```

PostgreSQL:

```text
event_quality_log | user_history | product_history | category_history
------------------+--------------+-----------------+-----------------
0                 | 79324        | 3744            | 126
```

해석:

- 정상 이벤트만 입력했으므로 quality log는 0이다.
- Redis와 PostgreSQL에 feature update가 정상 생성됐다.

## 결론

현재 로컬 MVP는 단일 순차 generator가 만든 약 90 eps 수준의 정상 이벤트 부하는 안정적으로 처리했다.

확인된 사실:

- T1 10 eps: 성공, final lag 0
- T2 실제 약 90 eps: 성공, final lag 0
- API/Kafka publish failure: 0
- validation failure: 0
- Flink checkpoint failure: 0
- Redis/PostgreSQL feature update 생성 정상

아직 말하면 안 되는 것:

- "500 eps를 안정 처리했다"는 결론은 낼 수 없다.
- 현재 generator가 500 eps 부하를 만들지 못했기 때문이다.

운영적 해석:

- API 수집과 validation job은 이번 범위에서 병목으로 보이지 않았다.
- aggregation job은 최종적으로 따라잡았지만, checkpoint duration outlier와 일시 lag가 관찰됐다.
- 다음 성능 실험은 generator를 concurrent sender로 확장한 뒤 100/300/500 eps를 다시 측정해야 한다.

## 다음 개선

Throughput 실험을 블로그에 강하게 쓰려면 generator에 아래 기능을 추가해야 한다.

- `--concurrency` 옵션
- 목표 rate를 worker별로 분산
- run 중 5초 단위 achieved eps 출력
- run 중 Kafka lag sampling 스크립트
- p50/p95/p99 API request latency 측정
