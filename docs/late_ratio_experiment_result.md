# Late Ratio Experiment Result

## 목적

이 실험은 late event 비율이 증가할 때 실시간 Feature Store 파이프라인이 어떻게 반응하는지 확인한다.

핵심 질문:

- late event는 API/Kafka/validation 단계에서 실패하는가?
- Flink aggregation 단계에서 late event가 `late-events-dlq`로 격리되는가?
- late ratio 증가가 aggregation lag와 checkpoint duration에 영향을 주는가?

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
- Aggregation ingest-delay late threshold: 120s

## 실험 조건

모든 run은 `mixed` mode를 사용하되 `duplicate_ratio=0`, `invalid_ratio=0`으로 고정했다.

| Run | late ratio | target rate | duration | target events | seed |
| --- | ---: | ---: | ---: | ---: | ---: |
| L1 | 0% | 100 eps | 120s | 12,000 | 2026052301 |
| L2 | 1% | 100 eps | 120s | 12,000 | 2026052302 |
| L3 | 5% | 100 eps | 120s | 12,000 | 2026052303 |
| L4 | 10% | 100 eps | 120s | 12,000 | 2026052304 |

## 실제 Late 수

동일 seed와 동일 generator 로직으로 실제 late event 수를 계산했다.

| Run | sent | late events | on-time events |
| --- | ---: | ---: | ---: |
| L1 | 12,000 | 0 | 12,000 |
| L2 | 12,000 | 122 | 11,878 |
| L3 | 12,000 | 532 | 11,468 |
| L4 | 12,000 | 1,155 | 10,845 |

전체 late event:

```text
expected late total = 1,809
```

## Generator 결과

| Run | sent | accepted | failed | elapsed seconds | achieved eps |
| --- | ---: | ---: | ---: | ---: | ---: |
| L1 | 12,000 | 12,000 | 0 | 136.701 | 87.78 |
| L2 | 12,000 | 12,000 | 0 | 135.524 | 88.55 |
| L3 | 12,000 | 12,000 | 0 | 134.776 | 89.04 |
| L4 | 12,000 | 12,000 | 0 | 134.913 | 88.95 |

해석:

- late ratio 0-10% 구간에서 API 수집 실패는 없었다.
- late event는 schema-valid event이므로 API에서 거절되지 않는다.
- 실제 부하는 약 88-89 eps 수준으로 유지됐다.

## API Metrics

최종 누적:

```json
{
  "api_requests_total": 48001,
  "event_ingest_success_total": 48000,
  "event_ingest_failure_total": 0,
  "kafka_publish_success_total": 48000,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 0
}
```

해석:

- 48,000건 모두 API validation과 Kafka publish를 통과했다.
- API/Kafka publish failure는 0이다.
- validation failure도 0이다.

## DLQ 결과

Invalid DLQ:

```text
invalid-user-events-dlq:0:0
invalid-user-events-dlq:1:0
invalid-user-events-dlq:2:0
```

Late DLQ 최초 확인:

```text
late-events-dlq:0:161
late-events-dlq:1:183
late-events-dlq:2:193
total: 537
```

60초 대기 후 Late DLQ:

```text
late-events-dlq:0:310
late-events-dlq:1:332
late-events-dlq:2:322
total: 964
```

해석:

- late DLQ는 증가했다.
- 하지만 기대 late event 1,809건 전체가 즉시 DLQ에 들어가지는 않았다.
- aggregation job이 밀려 있었기 때문에 late DLQ count도 시간이 지나며 증가했다.
- 따라서 이 run의 late DLQ count는 "최종 수렴값"이 아니라 "관찰 시점의 처리 진행값"으로 봐야 한다.

## Kafka Consumer Lag

Validation job 최종:

```text
GROUP                     TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
validation-enrichment-job raw-user-events 0          16118           16118           0
validation-enrichment-job raw-user-events 1          16078           16078           0
validation-enrichment-job raw-user-events 2          15804           15804           0
```

Aggregation job 최초 확인:

```text
GROUP                   TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
feature-aggregation-job clean-user-events 0          5275            16241           10966
feature-aggregation-job clean-user-events 1          7355            15889           8534
feature-aggregation-job clean-user-events 2          5375            15870           10495

total lag: 29,995
```

60초 대기 후 aggregation lag:

```text
GROUP                   TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
feature-aggregation-job clean-user-events 0          9629            16241           6612
feature-aggregation-job clean-user-events 1          8667            15889           7222
feature-aggregation-job clean-user-events 2          9640            15870           6230

total lag: 20,064
```

해석:

- validation job은 lag 0으로 수렴했다.
- aggregation job은 late ratio 실험 후 큰 lag가 남았다.
- 60초 동안 lag가 줄었으므로 job이 완전히 죽은 것은 아니다.
- 다만 aggregation throughput이 입력 처리량을 충분히 따라가지 못했고, checkpoint 지연과 함께 backlog가 쌓였다.

## Flink Checkpoint

Aggregation job checkpoint summary:

```text
completed checkpoints: 12
failed checkpoints: 0
in_progress checkpoints: 1
p50 checkpoint duration: 299 ms
p90 checkpoint duration: 238,122.9 ms
p95 checkpoint duration: 249,666 ms
p99 checkpoint duration: 249,666 ms
p95 checkpointed size: 41,773,467 bytes
latest completed checkpoint duration: 211,189 ms
```

긴 checkpoint:

```text
checkpoint 10 duration: 200,006 ms
checkpoint 11 duration: 249,666 ms
checkpoint 12 duration: 211,189 ms
```

해석:

- checkpoint 실패는 없었다.
- 그러나 checkpoint duration이 200-250초까지 튀었다.
- 이 checkpoint 지연은 aggregation lag와 late DLQ 지연 반영의 직접적인 원인 후보다.

## Redis/PostgreSQL 결과

Redis key count:

```text
13097
```

PostgreSQL:

```text
event_quality_log | user_history | product_history | category_history
------------------+--------------+-----------------+-----------------
0                 | 80091        | 1800            | 60
```

해석:

- quality log는 0이다.
- Redis/PostgreSQL feature update는 생성됐다.
- 단, aggregation lag가 남아 있으므로 최종 feature count로 해석하면 안 된다.

## 결론

Late event는 API/Kafka/validation 단계에서는 실패를 만들지 않았다.

확인된 사실:

- L1-L4 accepted: 100%
- API/Kafka publish failure: 0
- validation failure: 0
- invalid DLQ: 0
- validation final lag: 0
- late DLQ는 증가함

중요한 한계:

- late DLQ count가 기대 late event 1,809건에 즉시 도달하지 않았다.
- aggregation lag가 29,995건까지 쌓였고, 60초 후에도 20,064건이 남았다.
- checkpoint p95가 약 250초까지 튀었다.

따라서 이번 실험의 정확한 해석은 다음이다.

> Late event는 API나 validation에서 실패하지 않고 Flink aggregation 단계의 late DLQ 정책 대상이 된다. 다만 현재 로컬 MVP에서는 aggregation/checkpoint 병목 때문에 late DLQ 반영이 즉시 수렴하지 않았다. Late ratio 실험을 정확히 쓰려면 late DLQ count뿐 아니라 aggregation lag와 checkpoint duration을 반드시 함께 제시해야 한다.

## 블로그에 쓸 수 있는 메시지

좋은 메시지:

> late ratio가 증가하자 late DLQ가 증가했다. 이는 늦게 도착한 이벤트를 조용히 버리지 않고 별도 topic으로 관측 가능하게 격리한다는 점에서 운영적으로 의미가 있다.

주의해서 써야 할 메시지:

> late ratio 10%에서 DLQ가 정확히 10% 발생했다.

위 문장은 아직 쓰면 안 된다. aggregation lag가 남아 DLQ가 수렴하지 않았기 때문이다.

더 강한 실험으로 만들려면 다음 개선이 필요하다.

- run별로 stack을 reset해 checkpoint/state 누적 효과 제거
- run 종료 후 aggregation lag가 0이 될 때까지 대기
- 5초 단위 late DLQ count sampling
- 5초 단위 aggregation lag sampling
- checkpoint duration sampling
- `late_routed_total` metric 추가
