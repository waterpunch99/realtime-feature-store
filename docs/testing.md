# Testing

## 전제 조건

- Docker와 Docker Compose
- Python 3.11 이상
- Python 개발 환경에는 `pip`와 `venv` 또는 동등한 패키지 설치 수단 필요
- Java/Gradle은 호스트에 없어도 Docker Gradle 이미지로 검증 가능

## Python API 테스트

```bash
cd api
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
```

포함 테스트:

- 정상 health 응답
- 이벤트 필수값 검증
- invalid event quality log 처리
- Kafka publish delivery metadata 처리
- feature query service
- recommendation service

## Event Generator 테스트

```bash
cd event-generator
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
```

포함 테스트:

- 정상 이벤트 생성
- invalid 이벤트 생성
- duplicate 이벤트 생성
- late 이벤트 생성
- HTTP sender 동작

## Flink Java 테스트

호스트 Gradle 사용:

```bash
cd flink-jobs
gradle test
```

Docker Gradle 사용:

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle test --no-daemon
```

포함 테스트:

- invalid event validation
- future event validation
- user feature aggregation
- product feature aggregation
- category feature aggregation
- sink value conversion

## Build 검증

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

성공 시 다음 파일이 생성된다.

```text
flink-jobs/build/libs/flink-jobs.jar
```

## Docker Compose Smoke Test

1. 인프라 기동

```bash
scripts/run-local.sh
```

2. 상태 확인

```bash
docker compose ps
curl -fsS http://localhost:18081/overview
```

3. API 실행

```bash
scripts/run-api.sh
```

4. API health 확인

```bash
curl http://localhost:8000/health
```

5. Flink Job 빌드와 제출

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
scripts/submit-flink-jobs.sh
```

6. 이벤트 생성

```bash
scripts/run-generator.sh --mode normal --rate 10 --duration 60
```

7. 결과 확인

```bash
curl "http://localhost:8000/popular-products?window=10m&limit=20"
curl "http://localhost:8000/recommendations/users/u_10001?limit=20"
docker compose exec -T redis redis-cli zrevrange rank:product:popular:10m 0 10 withscores
docker compose exec -T postgres psql -U feature_store -d feature_store -c "SELECT count(*) FROM feature_product_history;"
```

실제 10,000건 혼합 이벤트를 흘린 end-to-end 실행 결과는
[E2E Smoke Test Result](e2e_smoke_test.md)에 기록한다.

## 현재 환경에서 확인된 제약

이 작업 환경에서는 시스템 Python에 `pip`, `pytest`, `venv`가 없어 Python pytest를 실행하지 못했다. 대신 `python3 -m compileall`로 구문 검증을 수행했다.

Flink Java 테스트와 fat jar 빌드는 Docker Gradle 이미지로 검증했다.
