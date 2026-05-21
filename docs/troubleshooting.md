# Troubleshooting

## Docker 포트 충돌

이 프로젝트는 로컬 충돌을 줄이기 위해 기본 서비스 포트를 호스트에서 다음처럼 노출한다.

- Kafka: `19092`
- PostgreSQL: `15432`
- Redis: `16379`
- Flink UI: `18081`

포트가 이미 사용 중이면 `docker-compose.yml`의 host port만 바꾼다. 컨테이너 내부 포트는 변경하지 않는다.

## Kafka 토픽이 없음

Kafka auto-create topic은 꺼져 있다.

```bash
scripts/create-topics.sh
```

토픽 확인:

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## API에서 Kafka publish 실패

확인 순서:

```bash
docker compose ps kafka
docker compose logs --tail=100 kafka
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
```

API는 Kafka publish delivery 결과를 확인한 뒤 성공 응답을 반환한다. 실패 시 503을 반환하고 structured log에 원인을 남긴다.

## Python pytest 실행 불가

다음 오류가 나면 Python 개발 도구가 없는 상태다.

```text
No module named pytest
No module named pip
ensurepip is not available
```

해결:

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
```

Debian/Ubuntu에서 venv가 없으면 `python3-venv` 패키지가 필요하다.

## Flink Job 빌드 환경 없음

호스트에 Java/Gradle이 없어도 Docker Gradle 이미지를 사용할 수 있다.

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle test --no-daemon
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace gradle:8.8-jdk17 gradle shadowJar --no-daemon
```

Docker Gradle이 root 소유의 `build/`, `.gradle/`을 남기면 다음처럼 제거한다.

```bash
docker run --rm -v "$PWD/flink-jobs:/workspace" -w /workspace --user root gradle:8.8-jdk17 rm -rf build .gradle
```

## Flink Job이 저장소에 쓰지 않음

확인 순서:

```bash
docker compose ps
curl -fsS http://localhost:18081/overview
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker compose exec -T redis redis-cli keys 'feature:*'
```

Validation Job과 Aggregation Job이 모두 실행 중이어야 Redis/PostgreSQL에 피처가 저장된다.

## 추천 결과가 비어 있음

추천 API는 Redis 인기 랭킹을 사용한다.

```bash
docker compose exec -T redis redis-cli zrevrange rank:product:popular:10m 0 20 withscores
```

랭킹이 비어 있으면 Flink Aggregation Job이 아직 window를 닫지 않았거나 이벤트가 충분히 들어오지 않은 상태일 수 있다. 10분 window는 1분 slide로 결과를 낸다.

