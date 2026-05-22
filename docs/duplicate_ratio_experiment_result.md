# Duplicate Ratio Experiment Result

## 목적

이 실험은 duplicate event 비율이 증가할 때 실시간 Feature Store 파이프라인이 어떻게 반응하는지 확인한다.

핵심 질문:

- duplicate event가 API/Kafka/Flink pipeline을 실패시키는가?
- `event_id` 기반 Flink dedup이 적용되는 구조에서 duplicate event가 aggregation으로 그대로 반영되지 않는가?
- duplicate ratio 증가가 lag나 checkpoint duration에 영향을 주는가?

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
- Flink parallelism: 1
- Kafka partitions: 3
- Checkpoint interval: 60s

## 실험 조건

모든 run은 `mixed` mode를 사용하되 `invalid_ratio=0`, `late_ratio=0`으로 고정했다.

| Run | duplicate ratio | target rate | duration | target events | seed |
| --- | ---: | ---: | ---: | ---: | ---: |
| D1 | 0% | 100 eps | 120s | 12,000 | 2026052201 |
| D2 | 1% | 100 eps | 120s | 12,000 | 2026052202 |
| D3 | 5% | 100 eps | 120s | 12,000 | 2026052203 |
| D4 | 10% | 100 eps | 120s | 12,000 | 2026052204 |

D4는 중간부터 achieved rate가 크게 떨어져 7,439건 수집 후 중단했다.

## 실제 Duplicate 수

동일 seed와 동일 generator 로직으로 실제 duplicate event 수를 계산했다.

| Run | sent | duplicate events | unique events |
| --- | ---: | ---: | ---: |
| D1 | 12,000 | 0 | 12,000 |
| D2 | 12,000 | 123 | 11,877 |
| D3 | 12,000 | 605 | 11,395 |
| D4 | 7,439 | 729 | 6,710 |

해석:

- D2, D3는 설정한 duplicate ratio와 거의 일치한다.
- D4는 부분 실행 기준으로 약 9.8% duplicate가 생성됐다.

## Generator 결과

| Run | sent | accepted | failed | elapsed seconds | achieved eps | status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| D1 | 12,000 | 12,000 | 0 | 133.172 | 90.11 | completed |
| D2 | 12,000 | 12,000 | 0 | 132.046 | 90.88 | completed |
| D3 | 12,000 | 12,000 | 0 | 131.821 | 91.03 | completed |
| D4 | 7,439 | 7,439 | 0 | interrupted | 약 36 eps까지 하락 | partial |

해석:

- D1-D3는 약 90 eps 수준에서 안정적으로 수집됐다.
- duplicate event는 schema-valid event이므로 API accepted count가 줄지 않는 것이 정상이다.
- D4는 중간에 achieved rate가 90 eps 근처에서 36 eps 수준까지 하락했다.

## API Metrics

최종 누적:

```json
{
  "api_requests_total": 43440,
  "event_ingest_success_total": 43439,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 43439,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

해석:

- 모든 전송 이벤트가 API validation과 Kafka publish를 통과했다.
- duplicate event는 API 단계에서 invalid로 처리되지 않는다.
- API/Kafka publish failure는 0이다.

## DLQ 결과

```text
invalid-user-events-dlq:0:0
invalid-user-events-dlq:1:0
invalid-user-events-dlq:2:0

late-events-dlq:0:0
late-events-dlq:1:0
late-events-dlq:2:0
```

해석:

- 이번 실험은 duplicate만 바꿨고 invalid/late는 0으로 고정했기 때문에 DLQ가 0인 것이 정상이다.
- duplicate는 DLQ 대상이 아니라 `event_id` dedup 대상이다.

## Kafka Consumer Lag

Validation job 최종:

```text
GROUP                     TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
validation-enrichment-job raw-user-events 0          14397           14397           0
validation-enrichment-job raw-user-events 1          14515           14515           0
validation-enrichment-job raw-user-events 2          14527           14527           0
```

Aggregation job 최종:

```text
GROUP                   TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
feature-aggregation-job clean-user-events 0          8282            14976           6694
feature-aggregation-job clean-user-events 1          7931            14417           6486
feature-aggregation-job clean-user-events 2          7787            14046           6259
```

해석:

- validation job은 최종 lag 0으로 수렴했다.
- aggregation job은 D4 이후 총 19,439건 lag가 남았다.
- 60초 대기 후에도 aggregation lag가 줄지 않았다.
- Flink job 자체는 RUNNING이지만 checkpoint가 in-progress 상태로 남아 aggregation이 정상적으로 따라잡지 못했다.

## Flink Checkpoint

Aggregation job checkpoint summary:

```text
completed checkpoints: 15
failed checkpoints: 0
in_progress checkpoints: 1
p50 checkpoint duration: 278 ms
p90 checkpoint duration: 115,394 ms
p95 checkpoint duration: 145,373 ms
p99 checkpoint duration: 145,373 ms
p95 checkpointed size: 41,259,068 bytes
latest completed checkpoint duration: 363 ms
```

긴 checkpoint:

```text
checkpoint 9  duration:  95,408 ms
checkpoint 10 duration: 145,373 ms
```

해석:

- checkpoint 실패는 없었지만 매우 긴 checkpoint가 발생했다.
- D4 중 generator achieved rate가 급락한 시점과 aggregation lag/checkpoint 지연이 함께 관찰됐다.
- 이 결과는 duplicate ratio 자체보다 stateful aggregation/checkpoint 비용 문제가 더 크다는 신호다.

## Redis/PostgreSQL 결과

최종 Redis key count:

```text
14165
```

PostgreSQL:

```text
event_quality_log | user_history | product_history | category_history
------------------+--------------+-----------------+-----------------
0                 | 96794        | 1980            | 66
```

해석:

- quality log는 0이다.
- Redis/PostgreSQL feature update는 생성됐다.
- 다만 aggregation lag가 남은 상태이므로 최종 feature count를 완전 처리 결과로 해석하면 안 된다.

## Dedup 해석

현재 Flink aggregation job은 다음 순서로 처리한다.

```text
cleanEvents
  -> keyBy(event_id)
  -> EventIdDedupFunction
  -> late router
  -> event-time window aggregation
```

따라서 duplicate event는 clean topic에는 들어가지만, 동일 `event_id`가 dedup state에 이미 있으면 aggregation으로 내려가지 않아야 한다.

이번 실험으로 확인한 것:

- duplicate event는 API/Kafka/validation을 실패시키지 않는다.
- invalid/late DLQ는 증가하지 않는다.
- validation job은 duplicate ratio 증가와 무관하게 lag 0으로 수렴한다.

이번 실험만으로 강하게 말하면 안 되는 것:

- "dedup이 피처 정확도를 완전히 보호했다"는 결론은 아직 약하다.
- 현재 결과에는 dedup drop count metric이 없다.
- feature correctness를 정량화하려면 dedup on/off 비교 또는 dedup-drop counter가 필요하다.

## 결론

D1-D3 기준으로 duplicate ratio 0-5%에서는 약 90 eps 부하를 API/Kafka/validation이 안정적으로 처리했다.

확인된 사실:

- D1-D3 accepted: 100%
- D1-D3 failed: 0
- invalid DLQ: 0
- late DLQ: 0
- validation final lag: 0
- Flink job state: RUNNING

주의할 사실:

- D4 10% run은 부분 실행이다.
- D4 중 achieved rate가 크게 떨어졌고 aggregation lag가 19,439건 남았다.
- checkpoint p95가 145초까지 튀었다.

채용/블로그 관점에서 가장 좋은 해석은 다음이다.

> Duplicate event 자체는 API나 validation의 실패 원인이 아니었다. 그러나 stateful dedup과 window aggregation을 가진 Flink job에서는 상태 크기와 checkpoint 지연이 실제 병목으로 드러났다. 따라서 dedup 효과를 정확도 관점에서 증명하려면 dedup drop count와 feature correctness metric을 추가해야 한다.

## 다음 개선

다음 실험 전에 아래 metric을 추가하는 것이 좋다.

- `dedup_seen_total`
- `dedup_dropped_total`
- `dedup_drop_ratio`
- aggregation input event count
- aggregation output feature update count
- run 중 5초 단위 Kafka lag sampling
- run 중 checkpoint duration sampling

이 지표가 있어야 duplicate ratio 증가에 따른 dedup 효과를 "추정"이 아니라 "측정"으로 말할 수 있다.
