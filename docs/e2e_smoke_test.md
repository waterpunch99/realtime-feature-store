# E2E Smoke Test Result

## 목적

이 문서는 로컬 Docker Compose 환경에서 이벤트 수집부터 Kafka, Flink, Redis,
PostgreSQL, 조회 API까지 end-to-end로 동작하는지 확인한 실행 기록이다.

## 실행 환경

- 실행일: 2026-05-21
- 인프라: Docker Compose
- Kafka: `confluentinc/cp-kafka:7.6.1`
- Flink: `flink:1.19-java17`
- PostgreSQL: `postgres:16`
- Redis: `redis:7`
- API 실행: `python:3.11-slim` 컨테이너
- Generator 실행: `python:3.11-slim` 컨테이너

## 준비

인프라 상태:

```text
rfs-kafka               Up, healthy
rfs-postgres            Up, healthy
rfs-redis               Up, healthy
rfs-flink-jobmanager    Up, healthy
rfs-flink-taskmanager   Up
```

Flink job jar 빌드:

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

결과:

```text
BUILD SUCCESSFUL in 1m 9s
```

Flink checkpoint volume 권한 이슈가 있으면 다음 명령으로 로컬 데모 환경의
checkpoint/savepoint 디렉터리 쓰기 권한을 조정한다.

```bash
docker compose exec -T flink-jobmanager chmod 777 /tmp/flink-checkpoints /tmp/flink-savepoints
```

Flink job 제출:

```bash
scripts/submit-flink-jobs.sh
```

제출 결과:

```text
validation-enrichment-job JobID: 0ce728d88f2607666d21a8d82d65a02c
feature-aggregation-job   JobID: 246458274981b661f4df2ebf3b5c39e8
```

## 입력 이벤트

혼합 품질 이벤트 10,000건을 API로 전송했다.

```bash
docker run --rm \
  --network container:<api-container-id> \
  -v "$PWD/event-generator:/workspace/event-generator" \
  -w /workspace/event-generator \
  python:3.11-slim \
  sh -c 'python -m pip install -q -e ".[dev]" && \
    python -m generator.main \
      --collector-url http://127.0.0.1:8000/events \
      --mode mixed \
      --rate 100 \
      --duration 100 \
      --duplicate-ratio 0.05 \
      --invalid-ratio 0.02 \
      --late-ratio 0.05 \
      --seed 20260521'
```

Generator 결과:

```text
completed sent=10000 accepted=9822 failed=178
```

해석:

- `accepted=9822`: FastAPI validation 통과 후 Kafka `raw-user-events` publish 성공
- `failed=178`: API validation에서 의도적으로 거절된 invalid 이벤트
- duplicate/late 이벤트는 API 단계에서는 유효 이벤트로 수신되고, 이후 Flink 단계에서 dedup 또는 late policy의 대상이 된다.

## API Metrics

```text
api_requests_total=10002
event_ingest_success_total=9822
event_ingest_failure_total=178
kafka_publish_success_total=9822
kafka_publish_failure_total=0
validation_failure_total=178
```

## Flink Job 상태

```text
validation-enrichment-job RUNNING
feature-aggregation-job   RUNNING
```

이전에 checkpoint directory 권한 문제로 실패한 validation job 기록이 1건 있었지만,
권한 조정 후 재제출한 job은 정상 실행 상태다.

## Kafka Consumer Lag

Validation job:

```text
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
raw-user-events 0          3283            3283            0
raw-user-events 1          3290            3290            0
raw-user-events 2          3249            3249            0
```

Aggregation job:

```text
TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
clean-user-events 0          3361            3361            0
clean-user-events 1          3176            3176            0
clean-user-events 2          3285            3285            0
```

## Redis 결과

Redis key count:

```text
3995
```

인기 상품 랭킹:

```text
p_00163  96
p_00024  92
p_00082  87
p_00119  85
p_00070  83
```

인기 카테고리 랭킹:

```text
c_grocery      1711
c_sports       1671
c_fashion      1670
c_books        1639
c_electronics  1591
```

## PostgreSQL 결과

```text
event_quality_log  user_history  product_history  category_history
178                5618          360              12
```

Invalid 이벤트 사유:

```text
reason_code                count
schema_validation_failed   131
required_field_missing     47
```

샘플 사용자 latest 피처:

```text
user_id  user_click_count_10m  user_view_count_10m  updated_at
u_09263  1                     0                    2026-05-21 14:34:16.860709+00
u_00411  1                     0                    2026-05-21 14:34:16.860699+00
u_04412  2                     0                    2026-05-21 14:34:16.860686+00
```

## API 조회 결과

`GET /popular-products?window=10m&limit=3`:

```json
{
  "window": "10m",
  "items": [
    {
      "id": "p_00163",
      "score": "96.0",
      "features": {
        "product_click_count_10m": 9,
        "product_ctr_10m": 0.642857,
        "product_view_count_10m": 14,
        "product_add_to_cart_count_10m": 5,
        "product_popularity_score_10m": 96,
        "window_start": "2026-05-21T14:22:00Z",
        "window_end": "2026-05-21T14:32:00Z"
      }
    }
  ]
}
```

## 확인된 제약

- API와 generator는 현재 작업 환경의 Python에 `pip`/`venv`가 없어 `python:3.11-slim` 컨테이너에서 실행했다.
- 현재 작업 환경에서는 호스트 `localhost:8000` 접근이 제한되어 API 컨테이너 내부에서 health/API 조회를 수행했다.
- `scripts/submit-flink-jobs.sh`는 streaming job 제출 후 다음 job으로 넘어가도록 detached mode가 필요해 `flink run -d`로 보정했다.
- checkpoint volume 권한이 기존 Docker volume 상태에 따라 문제될 수 있어, 로컬 데모용 권한 조정 명령을 기록했다.

## 결론

10,000건 혼합 이벤트 기준으로 수집, Kafka publish, Flink validation/aggregation,
Redis online feature, PostgreSQL history/latest, API ranking 조회까지 end-to-end
경로가 정상 동작했다. Kafka publish failure는 0건이었고, 의도적으로 생성한 invalid
이벤트 178건은 API validation 단계에서 거절되어 `event_quality_log`에 기록됐다.
