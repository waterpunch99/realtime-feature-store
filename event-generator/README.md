# Event Generator

Synthetic Event Generator는 Kafka에 직접 이벤트를 보내지 않는다. 실제 앱/클라이언트처럼 FastAPI Event Collector API에 HTTP 요청을 보낸다.

Generator는 `event_time`만 생성하며, `ingest_time`은 FastAPI Event Collector API가 서버 수신 시점에 부여한다.

## Run

```bash
python -m generator.main --mode normal --rate 10 --duration 60
python -m generator.main --mode mixed --rate 20 --duration 120 --duplicate-ratio 0.05 --invalid-ratio 0.02 --late-ratio 0.05
python -m generator.main --mode burst --rate 200 --duration 30
python -m generator.main --mode scenario --user-id u_10001 --category-id c_electronics --duration 60
```

기본 수집 API URL:

```text
http://localhost:8000/events
```

