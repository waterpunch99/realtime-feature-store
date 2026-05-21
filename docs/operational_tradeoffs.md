# Operational Tradeoffs

## 목적

이 문서는 로컬 MVP를 운영 시스템으로 확장할 때 반드시 설명해야 하는 장애 지점,
처리 보장, 재처리 절차, 상태 저장소 선택 기준을 정리한다. 면접에서는 이 프로젝트를
완전한 운영 플랫폼이 아니라 운영 제약을 의식한 실시간 피처 스토어 MVP로 설명한다.

## 처리 보장 요약

이 프로젝트는 end-to-end exactly-once를 단정하지 않는다.

현재 보장 범위:

- FastAPI는 Kafka delivery 결과를 확인한 뒤 성공 응답을 반환한다.
- Kafka producer는 idempotence, `acks=all`, retry를 사용한다.
- Flink는 checkpoint로 dedup/window state를 복구한다.
- Flink Kafka feature update sink는 `AT_LEAST_ONCE`이다.
- Redis/PostgreSQL sink는 Flink checkpoint transaction과 묶인 two-phase commit sink가 아니다.
- Redis latest는 key overwrite, PostgreSQL latest는 upsert, history는 unique constraint로 중복 write 영향을 줄인다.

따라서 장애 복구 중 일부 외부 sink write는 재시도될 수 있다. 현재 구조의 핵심 전략은
strong exactly-once가 아니라 idempotent write와 unique constraint를 이용한 중복 영향 완화다.

## 장애 시나리오

### API 수신 후 Kafka publish 전 장애

Outbox Pattern을 사용하지 않으므로 API가 이벤트를 받은 뒤 Kafka publish 전에 장애가 나면
해당 이벤트는 Kafka에 남지 않을 수 있다. 현재 MVP는 publish 성공 전까지 성공 응답을 반환하지
않기 때문에 클라이언트가 retry할 수 있다는 전제에 의존한다.

운영 확장:

- API DB transaction에 outbox row를 먼저 기록한다.
- 별도 relay가 outbox를 Kafka로 publish한다.
- outbox row 상태를 `pending`, `published`, `failed`로 관리한다.
- client idempotency key 또는 `event_id` unique constraint로 API retry 중복을 제어한다.

### API Kafka publish 성공 후 응답 전 장애

Kafka publish는 성공했지만 API 응답 전에 프로세스가 죽으면 클라이언트는 실패로 보고
같은 이벤트를 다시 보낼 수 있다. 현재 구조에서는 같은 `event_id`가 재전송되면 Flink dedup
state가 TTL 범위 내에서 중복 집계를 막는다.

운영 확장:

- 클라이언트가 안정적인 `event_id` 또는 idempotency key를 보내도록 계약화한다.
- API 단계에서도 최근 `event_id`를 short-term store에 기록해 중복 publish를 줄인다.
- Flink dedup TTL은 최대 재시도 기간보다 길게 잡는다.

### Flink 장애와 재시작

Flink checkpoint는 Kafka offset, dedup state, window state 복구에 사용된다. 다만 Redis와
PostgreSQL custom sink write는 checkpoint와 원자적으로 commit되지 않으므로 장애 직전 write가
재시도될 수 있다.

운영 확장:

- Kafka sink는 transactional delivery guarantee를 검토한다.
- PostgreSQL sink는 Flink two-phase commit sink 또는 staging table plus commit protocol을 검토한다.
- Redis는 latest overwrite 중심으로 유지하고, ranking은 window id를 포함한 versioned key를 검토한다.

### Redis 장애

Redis 장애 시 추천 API의 online feature 조회와 인기 랭킹 조회가 실패한다. 현재 MVP는 Redis를
online store로 보고 PostgreSQL을 직접 fallback으로 사용하지 않는다.

운영 확장:

- API에서 Redis read failure를 명확한 503으로 반환한다.
- 중요 endpoint는 PostgreSQL latest fallback을 둔다.
- Redis RDB/AOF, replica, Sentinel 또는 managed Redis를 사용한다.
- ranking key에는 TTL 또는 window versioning을 적용해 stale ranking 누적을 줄인다.

### PostgreSQL 장애

PostgreSQL 장애 시 latest/history 저장이 실패하면서 Flink sink가 fail하고 job이 재시작될 수 있다.
Redis write와 PostgreSQL write 사이에 부분 성공이 생길 수 있다.

운영 확장:

- Redis와 PostgreSQL write를 같은 job에서 직접 동기 수행하지 않고 feature update Kafka topic 뒤로 분리한다.
- Postgres writer job은 retry/backoff와 DLQ를 별도로 가진다.
- history table은 partitioning과 retention policy를 둔다.
- latest와 history write 실패를 각각 metric으로 노출한다.

### Kafka 장애 또는 Consumer Lag 증가

Kafka 장애 시 API publish가 실패하고 503을 반환한다. consumer lag가 증가하면 Redis/PostgreSQL
피처 freshness가 떨어진다.

운영 확장:

- API publish failure rate와 Kafka broker 상태를 alerting한다.
- `raw-user-events`, `clean-user-events` consumer lag를 job별로 모니터링한다.
- lag가 지속되면 Flink parallelism, Kafka partition 수, sink 처리량을 함께 조정한다.

## DLQ와 Replay

현재 DLQ topic:

- `invalid-user-events-dlq`: schema 또는 품질 검증 실패 이벤트
- `late-events-dlq`: `ingest_time - event_time` 기준 2분 초과 이벤트

Replay 원칙:

- invalid DLQ는 원본 이벤트를 바로 재처리하지 않는다. reason code별로 수정 가능 여부를 먼저 판단한다.
- late DLQ는 정책 변경 또는 backfill 시 별도 replay job으로 처리한다.
- replay job은 원래 topic에 무조건 재주입하지 않고, replay 전용 topic을 사용하는 편이 안전하다.
- replay 이벤트에는 `replayed_at`, `replay_reason`, `original_reason_code` 같은 audit field를 추가한다.
- replay 시에도 같은 `event_id`를 유지해 dedup 정책이 동작하게 한다.

Replay 절차 예시:

```text
DLQ topic
  -> inspect reason_code
  -> transform/fix if possible
  -> publish to replay topic
  -> run validation/aggregation with replay source
  -> compare feature counts and sink metrics
```

## Idempotency 전략

현재 idempotency 장치:

- API: Kafka idempotent producer, `acks=all`
- Flink: `event_id` 기준 dedup state TTL
- Redis: feature key overwrite
- PostgreSQL latest: primary key upsert
- PostgreSQL history: `(entity_id, window_size, window_start, window_end)` unique constraint

주의할 점:

- `event_id`가 매 retry마다 바뀌면 dedup이 동작하지 않는다.
- dedup TTL보다 늦게 재전송된 이벤트는 중복 반영될 수 있다.
- Redis sorted set ranking은 같은 member score overwrite에는 강하지만, stale window 결과 정리 정책이 별도로 필요하다.
- PostgreSQL history unique constraint는 같은 window 결과 중복 insert를 막지만, 잘못 계산된 값을 자동 보정하지는 않는다.

운영 확장:

- API contract에 stable `event_id`를 명시한다.
- producer retry 기간, client retry 기간, dedup TTL을 함께 설계한다.
- feature update에 `window_start`, `window_end`, `window_size`, `job_version`을 포함해 sink idempotency key로 사용한다.

## State Backend 확장

MVP 기본값은 `HashMapStateBackend`이다. 로컬 데모에서는 설정이 단순하고 상태 크기가 작아 적합하다.

RocksDB 전환 기준:

- key cardinality가 크게 증가한다.
- dedup state 또는 window state가 TaskManager heap을 압박한다.
- 24시간 이상 장시간 window를 운영한다.
- checkpoint 크기와 복구 시간이 중요해진다.
- TaskManager restart 후 안정적인 state restore가 필요하다.

전환 시 함께 볼 항목:

- checkpoint interval과 timeout
- checkpoint storage 위치와 보존 정책
- state TTL cleanup 정책
- RocksDB local disk 성능
- backpressure와 checkpoint alignment time

## Observability

현재 MVP는 `/metrics`와 Docker/Flink/Kafka CLI 확인에 의존한다.

운영에서 필요한 metric:

- API request count, validation failure count, Kafka publish failure count
- Kafka consumer lag by topic/group/partition
- Flink checkpoint success/failure, checkpoint duration, state size
- Flink backpressure, restart count, busy time
- Redis command latency, memory usage, key count
- PostgreSQL insert/upsert latency, connection pool usage, deadlock/error count
- feature freshness: latest `window_end`와 현재 시간 차이

Alert 기준 예시:

- Kafka publish failure가 5분 이상 0보다 큼
- validation failure ratio가 평소 대비 급증
- consumer lag가 계속 증가
- Flink checkpoint failure가 연속 발생
- feature freshness가 SLA를 초과
- Redis/PostgreSQL read/write latency가 임계값 초과

## 운영 전환 우선순위

1. API Outbox 또는 client idempotency key 도입
2. Prometheus/Grafana 기반 metric 수집과 alerting
3. Redis/PostgreSQL sink 분리와 retry/DLQ writer 구조 도입
4. RocksDB state backend 전환과 checkpoint storage 운영화
5. DLQ replay tool과 audit field 추가
6. schema registry 또는 event versioning 도입
7. ranking key TTL/window versioning 도입

## 면접 답변 포인트

- 이 MVP는 exactly-once를 과장하지 않고, 중복 영향 완화 전략을 명시했다.
- checkpoint는 Flink state 복구를 위한 장치이지 모든 외부 sink 원자성을 자동 보장하지 않는다.
- Outbox를 제외한 이유는 MVP 단순성이며, 운영 전환 시 가장 먼저 도입할 후보로 보고 있다.
- late DLQ는 watermark side output이 아니라 `ingest_time - event_time` 기준 business filter이다.
- Redis는 online serving, PostgreSQL은 latest/history 보존으로 역할을 분리했다.
- 운영 확장 시 replay, observability, state backend, sink transactionality를 순서대로 강화한다.
