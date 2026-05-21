#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export RFS_DATABASE_URL="${RFS_DATABASE_URL:-postgresql+asyncpg://feature_store:feature_store@localhost:15432/feature_store}"
export RFS_REDIS_URL="${RFS_REDIS_URL:-redis://localhost:16379/0}"
export RFS_KAFKA_BOOTSTRAP_SERVERS="${RFS_KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

cd "${ROOT_DIR}/api"

exec python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port "${API_PORT:-8000}"

