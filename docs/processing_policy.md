# Processing Policy

## 시간 기준

- 집계 기준 시간은 `event_time`이다.
- `ingest_time`도 저장한다.
- 지연 판단은 `ingest_time - event_time` 기준이다.
- watermark out-of-orderness 설정은 2분이다.
- MVP의 late DLQ 라우팅은 watermark side output이 아니라 `ingest_time - event_time` 기준의 business lateness filter이다.

## 지연 이벤트 정책

- 2분 이내 지연 이벤트는 정상 집계에 반영한다.
- 2분 초과 지연 이벤트는 `late-events-dlq`로 보낸다.
- DLQ reason code는 `event_time_late_beyond_ingest_delay`를 사용한다.
- PostgreSQL `late_event_log` 테이블은 지연 이벤트 이력 저장소로 준비되어 있으며, 운영 확장 시 `late-events-dlq` consumer 또는 별도 Flink sink가 이 테이블에 기록한다.
- 현재 시간보다 10분 이상 미래인 `event_time`은 `invalid-user-events-dlq`로 보낸다.
- 24시간 이상 과거인 `event_time`은 invalid 또는 late로 분류한다.

## 중복 이벤트 정책

- FastAPI Kafka producer는 idempotent 설정을 사용한다.
- Kafka producer는 `acks=all`과 retries를 설정한다.
- Flink checkpoint를 활성화한다.
- Flink에서 `event_id` 기준 dedup state를 사용한다.
- dedup state에는 TTL을 둔다.
- PostgreSQL sink는 upsert를 사용한다.
- Redis sink는 feature key 기준 idempotent write를 사용한다.

이 프로젝트는 완전한 end-to-end exactly-once를 단정하지 않는다. Kafka idempotent producer, Flink checkpoint, event_id dedup, sink upsert를 조합해 중복 이벤트가 피처에 중복 반영되지 않도록 설계한다.

## Flink State Backend 정책

MVP 기본 State Backend는 `HashMapStateBackend`이다.

기본 설정:

- state backend: `hashmap`
- checkpoint enabled: `true`
- checkpoint interval: `60s`
- checkpoint path: `/tmp/flink-checkpoints`
- savepoint path: `/tmp/flink-savepoints`
- dedup state TTL: 기본 `25h`, 설정값으로 분리

## HashMapStateBackend 선택 이유

- 로컬 Docker Compose 기반 MVP에서 설정이 단순하다.
- 작은 상태 크기와 제한된 이벤트 볼륨을 전제로 빠르게 데모할 수 있다.
- 포트폴리오 MVP에서 window aggregation, event-time, watermark, dedup 로직 자체를 보여주는 데 집중할 수 있다.

## RocksDB 전환 기준

다음 조건에서는 `EmbeddedRocksDBStateBackend` 전환을 고려한다.

- dedup state 또는 window state가 메모리에 부담을 줄 정도로 커지는 경우
- key cardinality가 크게 증가하는 경우
- 장시간 window와 대량 이벤트를 운영 수준으로 처리해야 하는 경우
- TaskManager 장애 복구와 대규모 checkpoint 안정성이 더 중요해지는 경우

State Backend, checkpoint interval, checkpoint path, savepoint path는 설정값으로 분리해 전환 비용을 낮춘다.

## Outbox Pattern 미사용 한계

Outbox Pattern을 사용하지 않기 때문에 API가 이벤트를 수신한 뒤 Kafka publish에 실패할 수 있다. MVP에서는 publish 성공 확인 전까지 성공 응답을 반환하지 않는다.

한계:

- API 프로세스 장애와 Kafka publish 실패 사이의 복구 자동화가 제한적이다.
- DB transaction과 Kafka publish를 하나의 원자적 작업으로 묶지 않는다.
- 실패 이벤트 재처리는 structured log와 metrics 기반 운영 절차에 의존한다.

이 한계는 README와 처리 정책 문서에 명시하고, 운영 확장 시 Outbox 또는 inbox/retry queue 패턴 도입을 검토할 수 있다.

## Window Aggregation 정책

- 10분 sliding window, 1분 slide를 구현한다.
- 1시간 sliding window, 5분 slide를 구현한다.
- 24시간 window는 schema와 문서에 확장 대상으로 남긴다.
- 집계 결과는 Kafka feature update 토픽, Redis, PostgreSQL latest/history에 저장한다.
- Redis latest write는 같은 feature key를 overwrite하는 방식으로 idempotent하게 동작한다.
- PostgreSQL latest는 upsert, history는 window unique constraint를 사용한다.
