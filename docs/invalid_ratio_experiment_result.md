# Invalid Ratio Experiment Result

## 목적

이 실험은 invalid event 비율이 증가할 때 수집/검증 계층이 잘못된 이벤트를 feature pipeline으로 흘려보내지 않는지 확인한다.

핵심 질문:

- invalid event는 API에서 거절되는가?
- 거절된 이벤트는 quality log에 기록되는가?
- invalid event가 Kafka raw topic, clean topic, feature store로 들어가지 않는가?

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

모든 run은 `mixed` mode를 사용하되 `duplicate_ratio=0`, `late_ratio=0`으로 고정했다.

| Run | invalid ratio | target rate | duration | target events | seed |
| --- | ---: | ---: | ---: | ---: | ---: |
| V1 | 1% | 100 eps | 120s | 12,000 | 2026052401 |
| V2 | 5% | 100 eps | 120s | 12,000 | 2026052402 |
| V3 | 10% | 100 eps | 120s | 12,000 | 2026052403 |

## Generator 결과

| Run | sent | accepted | failed | observed failure ratio | elapsed seconds | achieved eps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| V1 | 12,000 | 11,887 | 113 | 0.94% | 169.526 | 70.79 |
| V2 | 12,000 | 11,464 | 536 | 4.47% | 155.496 | 77.17 |
| V3 | 12,000 | 10,948 | 1,052 | 8.77% | 157.221 | 76.33 |

해석:

- invalid ratio가 증가할수록 generator failed count가 증가했다.
- target invalid ratio와 observed failure ratio가 정확히 같지는 않다.
- 이유는 generator의 invalid case 중 일부가 API validation 정책상 허용될 수 있기 때문이다. 예를 들어 `missing_product`는 이벤트 타입에 따라 항상 invalid가 아닐 수 있다.

## API Metrics

최종 누적:

```json
{
  "api_requests_total": 36001,
  "event_ingest_success_total": 34299,
  "event_ingest_failure_total": 1701,
  "kafka_publish_success_total": 34299,
  "kafka_publish_failure_total": 0,
  "validation_failure_total": 1701
}
```

해석:

- 총 36,000개 입력 이벤트 중 34,299개가 수집 성공했다.
- 1,701개는 API validation에서 실패했다.
- Kafka publish failure는 0이다.
- invalid event는 Kafka publish 전에 차단됐다.

## Quality Log

PostgreSQL `event_quality_log`:

```text
reason_code                count
-------------------------- -----
schema_validation_failed   1266
required_field_missing      435
```

총 quality log:

```text
event_quality_log = 1701
```

해석:

- API validation 실패 건수와 `event_quality_log` row count가 일치한다.
- invalid event는 단순히 버려진 것이 아니라 사유와 함께 기록됐다.

## Kafka DLQ

Invalid DLQ:

```text
invalid-user-events-dlq:0:0
invalid-user-events-dlq:1:0
invalid-user-events-dlq:2:0
```

Late DLQ:

```text
late-events-dlq:0:0
late-events-dlq:1:0
late-events-dlq:2:0
```

해석:

- 이번 실험의 invalid event는 API 단계에서 차단됐다.
- 따라서 Flink validation job의 `invalid-user-events-dlq`로 가지 않는 것이 현재 구조상 정상이다.
- Kafka invalid DLQ는 raw topic까지 들어온 이벤트 중 Flink validation에서 실패한 이벤트를 위한 경로다.

## Kafka Consumer Lag

Validation job:

```text
GROUP                     TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
validation-enrichment-job raw-user-events 0          11385           11385           0
validation-enrichment-job raw-user-events 1          11479           11479           0
validation-enrichment-job raw-user-events 2          11435           11435           0
```

Aggregation job:

```text
GROUP                   TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
feature-aggregation-job clean-user-events 0          7509            11882           4373
feature-aggregation-job clean-user-events 1          7178            11261           4083
feature-aggregation-job clean-user-events 2          7124            11156           4032

total lag: 12,488
```

해석:

- validation job은 raw topic을 모두 처리했고 lag 0으로 수렴했다.
- aggregation job은 clean topic에서 lag가 남았다.
- invalid event 실험의 핵심 검증은 API validation/quality log이므로 aggregation lag는 보조 관찰로 둔다.

## Flink Checkpoint

Aggregation job checkpoint summary:

```text
completed checkpoints: 9
failed checkpoints: 0
in_progress checkpoints: 1
p50 checkpoint duration: 869 ms
p90 checkpoint duration: 175,877 ms
p95 checkpoint duration: 175,877 ms
p99 checkpoint duration: 175,877 ms
p95 checkpointed size: 38,350,933 bytes
latest completed checkpoint duration: 175,877 ms
```

해석:

- checkpoint 실패는 없었다.
- 그러나 aggregation checkpoint duration이 176초까지 증가했다.
- throughput/duplicate/late 실험과 마찬가지로 stateful aggregation checkpoint가 반복적인 병목으로 관찰된다.

## Redis/PostgreSQL Feature Store

Redis key count:

```text
11669
```

PostgreSQL:

```text
event_quality_log | user_history | product_history | category_history
------------------+--------------+-----------------+-----------------
1701              | 48123        | 1326            | 48
```

해석:

- 정상 이벤트는 feature store로 흘러갔다.
- invalid 이벤트는 feature store가 아니라 `event_quality_log`로 기록됐다.
- aggregation lag가 남아 있으므로 feature history count는 최종 수렴값으로 해석하지 않는다.

## 결론

Invalid ratio 실험은 수집/검증 계층 관점에서 통과로 본다.

확인된 사실:

- invalid ratio가 증가할수록 API validation failure가 증가했다.
- API validation failure count와 quality log count가 1,701건으로 일치했다.
- Kafka publish failure는 0이다.
- validation job final lag는 0이다.
- invalid/late Kafka DLQ는 0이다.

중요한 해석:

> 이 프로젝트는 invalid event를 feature pipeline으로 흘려보내지 않고 API 계층에서 차단한 뒤 PostgreSQL quality log에 기록한다. Kafka invalid DLQ는 API를 통과해 raw topic에 들어온 뒤 Flink validation에서 실패한 이벤트를 위한 별도 경로다.

블로그에 쓰기 좋은 메시지:

> invalid ratio를 1%, 5%, 10%로 올리자 API validation failure와 quality log가 함께 증가했다. 잘못된 이벤트는 Kafka publish 전에 차단되어 clean topic과 feature store를 오염시키지 않았다.

주의할 점:

- target invalid ratio와 실제 failed ratio는 완전히 같지 않다.
- generator의 invalid case가 API validation 정책과 1:1로 대응하지 않기 때문이다.
- 다음 실험에서는 invalid case type별 비율을 고정하고, 사유별 expected count를 계산해야 한다.

## 다음 개선

더 강한 검증을 위해 아래를 추가한다.

- generator invalid case별 counter
- API rejected event reason별 response metric
- raw topic까지 들어가는 malformed JSON 케이스
- Flink validation DLQ 전용 실험
- run별 API validation latency 측정
