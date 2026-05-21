#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

docker compose up -d
"${ROOT_DIR}/scripts/create-topics.sh"

docker compose ps

cat <<'EOF'

Local infrastructure is running.

Endpoints:
  Kafka external bootstrap: localhost:19092
  PostgreSQL:              localhost:15432
  Redis:                   localhost:16379
  Flink Web UI:            http://localhost:18081

Run API:
  scripts/run-api.sh

Run generator:
  scripts/run-generator.sh --mode normal --rate 10 --duration 60
EOF

