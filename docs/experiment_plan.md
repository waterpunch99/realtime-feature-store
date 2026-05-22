# Experiment Plan: Real-time Feature Store Reliability

## 블로그 주제

이 실험 글의 중심 질문은 다음 하나로 고정한다.

> Kafka, Flink, Redis 기반 실시간 Feature Store는 이벤트 품질이 나빠져도 정확하고 빠른 피처를 제공할 수 있는가?

여러 실험을 단순히 나열하지 않는다. 모든 실험은 `정확도`, `지연시간`, `운영 안정성`이라는 세 관점으로 해석한다.

## 실험 순서

### 0. 기준선 재현

가장 먼저 아무 품질 오류도 없는 정상 이벤트만 흘려 기준선을 만든다.

목적:

- 로컬 인프라, API, Kafka, Flink, Redis, PostgreSQL이 정상 연결되는지 확인한다.
- 이후 실험 결과와 비교할 기준 latency, lag, feature count를 확보한다.

실행 예시:

```bash
scripts/reset-local.sh
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

API는 별도 터미널에서 실행한다.

```bash
scripts/run-api.sh
```

Flink job 제출과 generator 실행은 다른 터미널에서 수행한다.

```bash
scripts/submit-flink-jobs.sh
scripts/run-generator.sh --mode normal --rate 100 --duration 120 --seed 20260522
```

측정값:

- generator `sent`, `accepted`, `failed`
- API `/metrics`
- Kafka consumer lag
- Redis key count, popular ranking
- PostgreSQL history row count
- Flink job 상태와 checkpoint 상태

판정:

- API 실패율이 0%에 가까운가?
- validation/aggregation consumer lag가 최종적으로 0으로 수렴하는가?
- Redis와 PostgreSQL에 feature update가 생성되는가?

### 1. Throughput 증가 실험

정상 이벤트만 사용해서 시스템의 안정 처리 범위를 먼저 확인한다.

목적:

- 품질 이슈가 없는 상태에서 처리량 증가가 latency와 lag에 미치는 영향을 측정한다.
- 뒤의 duplicate/late 실험에서 성능 저하가 품질 이슈 때문인지 단순 부하 때문인지 분리한다.

실험 조건:

| Run | rate | duration | mode |
| --- | ---: | ---: | --- |
| T1 | 10 eps | 120s | normal |
| T2 | 100 eps | 120s | normal |
| T3 | 500 eps | 120s | normal |

실행 예시:

```bash
scripts/run-generator.sh --mode normal --rate 10 --duration 120 --seed 20260522
scripts/run-generator.sh --mode normal --rate 100 --duration 120 --seed 20260522
scripts/run-generator.sh --mode normal --rate 500 --duration 120 --seed 20260522
```

측정값:

- accepted events per second
- API failure ratio
- Kafka lag peak/final
- Redis/PostgreSQL update count
- Flink checkpoint duration

블로그에서 답할 질문:

- 이 로컬 MVP는 몇 eps까지 lag 없이 따라가는가?
- 병목은 API publish, Kafka lag, Flink aggregation, Redis/PostgreSQL sink 중 어디에 먼저 나타나는가?

### 2. Duplicate Ratio 실험

두 번째 핵심 실험이다. 실시간 feature aggregation에서 duplicate가 count/sum 계열 피처를 직접 오염시키기 때문이다.

목적:

- duplicate 이벤트가 증가할 때 dedup이 피처 정확도를 얼마나 보호하는지 확인한다.
- dedup state를 사용하는 비용이 latency나 checkpoint에 반영되는지 본다.

실험 조건:

| Run | rate | duration | duplicate_ratio | invalid_ratio | late_ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| D1 | 100 eps | 120s | 0.00 | 0.00 | 0.00 |
| D2 | 100 eps | 120s | 0.01 | 0.00 | 0.00 |
| D3 | 100 eps | 120s | 0.05 | 0.00 | 0.00 |
| D4 | 100 eps | 120s | 0.10 | 0.00 | 0.00 |

실행 예시:

```bash
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0.00 --invalid-ratio 0 --late-ratio 0 --seed 20260522
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0.01 --invalid-ratio 0 --late-ratio 0 --seed 20260522
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0.05 --invalid-ratio 0 --late-ratio 0 --seed 20260522
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0.10 --invalid-ratio 0 --late-ratio 0 --seed 20260522
```

측정값:

- generator accepted count
- clean-user-events input count
- feature update count
- Redis ranking score 변화
- PostgreSQL latest/history count
- checkpoint duration/size 변화

해석 포인트:

- dedup이 없었다면 duplicate ratio만큼 count feature가 과대 집계될 수 있다.
- 현재 Flink job은 `event_id` 기준 stateful dedup을 수행하므로, duplicate가 Redis 최종 피처에 중복 반영되지 않아야 한다.
- 단, dedup은 state를 사용하므로 checkpoint 비용 증가 여부를 함께 봐야 한다.

### 3. Late Ratio 실험

세 번째 핵심 실험이다. late event는 실시간 feature store에서 정확도와 freshness 사이의 trade-off를 만든다.

목적:

- late 이벤트 비율이 증가할 때 late DLQ가 의도대로 증가하는지 확인한다.
- 늦게 도착한 이벤트를 무조건 반영하지 않는 정책이 피처 freshness를 보호하는지 본다.

실험 조건:

| Run | rate | duration | duplicate_ratio | invalid_ratio | late_ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| L1 | 100 eps | 120s | 0.00 | 0.00 | 0.00 |
| L2 | 100 eps | 120s | 0.00 | 0.00 | 0.01 |
| L3 | 100 eps | 120s | 0.00 | 0.00 | 0.05 |
| L4 | 100 eps | 120s | 0.00 | 0.00 | 0.10 |

실행 예시:

```bash
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0 --invalid-ratio 0 --late-ratio 0.01 --seed 20260522
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0 --invalid-ratio 0 --late-ratio 0.05 --seed 20260522
scripts/run-generator.sh --mode mixed --rate 100 --duration 120 --duplicate-ratio 0 --invalid-ratio 0 --late-ratio 0.10 --seed 20260522
```

측정값:

- late-events-dlq message count
- clean-user-events count 대비 late DLQ ratio
- Redis/PostgreSQL에 반영된 feature count
- Kafka lag
- Flink job backpressure 또는 checkpoint 이상 여부

해석 포인트:

- API는 late event를 schema-valid 이벤트로 받아들이고, Flink aggregation 단계에서 ingest delay 기준으로 late DLQ에 보낸다.
- `late_ratio`가 증가할수록 late DLQ 비율이 선형에 가깝게 증가해야 한다.
- 이 결과는 "늦게 온 이벤트를 조용히 버리는 시스템"이 아니라 "정책적으로 격리하고 관측 가능한 시스템"이라는 메시지를 만든다.

### 4. Invalid Ratio 실험

이 실험은 validation job과 API 품질 검증을 설명하기 위한 보조 실험이다.

목적:

- API validation 또는 Flink validation 단계에서 invalid 이벤트가 정상 이벤트와 분리되는지 확인한다.
- 잘못된 이벤트가 clean topic과 feature store로 흘러가지 않는지 본다.

실험 조건:

| Run | rate | duration | duplicate_ratio | invalid_ratio | late_ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| V1 | 100 eps | 120s | 0.00 | 0.01 | 0.00 |
| V2 | 100 eps | 120s | 0.00 | 0.05 | 0.00 |
| V3 | 100 eps | 120s | 0.00 | 0.10 | 0.00 |

측정값:

- generator failed count
- API `validation_failure_total`
- PostgreSQL `event_quality_log` count
- invalid-user-events-dlq count
- clean-user-events count

해석 포인트:

- invalid 이벤트는 feature correctness를 깨기 전에 수집/검증 계층에서 차단되어야 한다.
- 이 실험은 메인 주제의 보조 근거로 짧게 다룬다.

### 5. Redis Serving Latency 실험

이 실험은 온라인 feature store로서 Redis가 조회 요구사항을 만족하는지 확인하는 보조 실험이다.

목적:

- feature query API의 p50/p95/p99 latency를 측정한다.
- Redis 조회가 추천 API의 online serving 병목인지 확인한다.

실험 조건:

- feature 데이터가 충분히 쌓인 뒤 실행한다.
- 단일 user feature 조회와 popular ranking 조회를 분리해서 측정한다.

측정 대상:

```bash
curl "http://localhost:8000/features/users/u_10001"
curl "http://localhost:8000/popular-products?window=10m&limit=20"
curl "http://localhost:8000/recommendations/users/u_10001?limit=20"
```

측정값:

- p50 latency
- p95 latency
- p99 latency
- error ratio

해석 포인트:

- Redis 자체 latency와 FastAPI serialization/network overhead를 구분해서 설명한다.
- 블로그 1편에서는 짧게 다루고, 성능 튜닝 글을 따로 쓸 경우 더 깊게 다룬다.

### 6. Flink Checkpoint Interval 실험

가장 마지막에 수행한다. 앞선 실험으로 안정적인 입력 조건을 확보한 뒤 checkpoint interval만 바꿔야 해석이 가능하다.

목적:

- checkpoint interval이 처리 성능, checkpoint duration, 장애 복구 안정성에 미치는 영향을 본다.
- "짧은 checkpoint가 항상 좋다"가 아니라 비용과 복구 지점 간 trade-off가 있음을 보여준다.

실험 조건:

| Run | rate | duration | checkpoint.interval.ms |
| --- | ---: | ---: | ---: |
| C1 | 100 eps | 180s | 5000 |
| C2 | 100 eps | 180s | 10000 |
| C3 | 100 eps | 180s | 30000 |
| C4 | 100 eps | 180s | 60000 |

실행 예시:

```bash
scripts/reset-local.sh
CHECKPOINT_INTERVAL_MS=5000 scripts/submit-flink-jobs.sh
scripts/run-generator.sh --mode normal --rate 100 --duration 180 --seed 20260522

scripts/reset-local.sh
CHECKPOINT_INTERVAL_MS=10000 scripts/submit-flink-jobs.sh
scripts/run-generator.sh --mode normal --rate 100 --duration 180 --seed 20260522

scripts/reset-local.sh
CHECKPOINT_INTERVAL_MS=30000 scripts/submit-flink-jobs.sh
scripts/run-generator.sh --mode normal --rate 100 --duration 180 --seed 20260522

scripts/reset-local.sh
CHECKPOINT_INTERVAL_MS=60000 scripts/submit-flink-jobs.sh
scripts/run-generator.sh --mode normal --rate 100 --duration 180 --seed 20260522
```

측정값:

- checkpoint count
- checkpoint duration
- failed checkpoint count
- Kafka lag
- feature update count
- 장애 재시작 후 중복/유실 여부

해석 포인트:

- interval이 짧으면 복구 지점은 촘촘해지지만 checkpoint overhead가 증가할 수 있다.
- interval이 길면 평상시 overhead는 줄 수 있지만 장애 시 재처리 구간이 길어진다.

## 최종 블로그 구조

### 제목 후보

`Kafka-Flink-Redis 기반 실시간 Feature Store의 정확도와 지연시간 검증`

또는

`실시간 Feature Store 신뢰성 실험: Duplicate와 Late Event는 피처 값을 얼마나 흔드는가`

### 글 흐름

1. 문제 정의
   - 실시간 feature store는 빠른 처리만으로 충분하지 않다.
   - duplicate, late, invalid 이벤트가 count/ranking feature를 오염시킬 수 있다.

2. 시스템 구조
   - Generator -> FastAPI -> Kafka -> Flink -> Redis/PostgreSQL
   - raw, clean, invalid DLQ, late DLQ topic 분리
   - event-time window aggregation과 `event_id` dedup

3. 실험 환경
   - Docker Compose 로컬 환경
   - Kafka 3 partitions
   - Flink parallelism 1
   - Redis online store
   - PostgreSQL offline/history store

4. 기준선
   - 정상 이벤트에서 lag와 feature update가 안정적으로 수렴하는지 확인

5. Throughput 실험
   - 10/100/500 eps 결과
   - 안정 처리 범위와 병목 위치 해석

6. Duplicate 실험
   - duplicate ratio별 feature correctness 영향
   - dedup state의 효과와 비용

7. Late Event 실험
   - late ratio별 DLQ 비율
   - freshness와 correctness trade-off

8. 보조 실험
   - invalid ratio
   - Redis serving latency
   - checkpoint interval

9. 결론
   - 안정적으로 처리 가능한 eps
   - 피처 정확도를 깨는 주요 요인
   - 운영 시 권장 설정
   - 다음 개선점

## 결과 기록 템플릿

각 실험은 아래 형식으로 기록한다.

```text
Run ID:
Date:
Git commit:
Input:
  mode:
  rate:
  duration:
  duplicate_ratio:
  invalid_ratio:
  late_ratio:
  seed:
Flink config:
  checkpoint_interval_ms:
  watermark_delay_seconds:
  dedup_ttl_hours:
Results:
  sent:
  accepted:
  failed:
  api_validation_failure_total:
  kafka_publish_failure_total:
  raw_topic_final_lag:
  clean_topic_final_lag:
  invalid_dlq_count:
  late_dlq_count:
  redis_key_count:
  postgres_user_history_count:
  postgres_product_history_count:
  postgres_category_history_count:
  checkpoint_count:
  checkpoint_duration_p95_ms:
Notes:
```

## 냉정한 우선순위

블로그 1편에 전부 깊게 넣지 않는다. 가장 강한 실험은 아래 세 개다.

1. Throughput 증가에 따른 lag와 feature update 안정성
2. Duplicate ratio 증가에 따른 dedup 효과
3. Late ratio 증가에 따른 late DLQ와 freshness/correctness trade-off

Redis latency와 checkpoint interval은 1편에서는 보조 결과로 짧게 넣고, 수치가 의미 있게 나오면 2편으로 분리한다.
