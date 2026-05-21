#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_JOBMANAGER="${FLINK_JOBMANAGER:-flink-jobmanager:8081}"
VALIDATION_JOB_JAR="${VALIDATION_JOB_JAR:-${ROOT_DIR}/flink-jobs/build/libs/flink-jobs.jar}"
AGGREGATION_JOB_JAR="${AGGREGATION_JOB_JAR:-${ROOT_DIR}/flink-jobs/build/libs/flink-jobs.jar}"
VALIDATION_JOB_CLASS="${VALIDATION_JOB_CLASS:-com.example.featurestore.jobs.ValidationEnrichmentJob}"
AGGREGATION_JOB_CLASS="${AGGREGATION_JOB_CLASS:-com.example.featurestore.jobs.FeatureAggregationJob}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
POSTGRES_JDBC_URL="${POSTGRES_JDBC_URL:-jdbc:postgresql://postgres:5432/feature_store}"
POSTGRES_USER="${POSTGRES_USER:-feature_store}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-feature_store}"
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"

cd "${ROOT_DIR}"

if [[ ! -f "${VALIDATION_JOB_JAR}" ]]; then
  echo "Validation job jar not found: ${VALIDATION_JOB_JAR}" >&2
  echo "Build Flink jobs in STEP 7/8 before submitting." >&2
  exit 1
fi

docker compose cp "${VALIDATION_JOB_JAR}" flink-jobmanager:/tmp/validation-job.jar
docker compose exec -T flink-jobmanager flink run \
  -m "${FLINK_JOBMANAGER}" \
  -c "${VALIDATION_JOB_CLASS}" \
  /tmp/validation-job.jar \
  --bootstrap.servers "${KAFKA_BOOTSTRAP_SERVERS}" \
  --raw.topic raw-user-events \
  --clean.topic clean-user-events \
  --invalid.topic invalid-user-events-dlq \
  --group.id validation-enrichment-job

if [[ "${AGGREGATION_JOB_JAR}" != "${VALIDATION_JOB_JAR}" ]]; then
  if [[ ! -f "${AGGREGATION_JOB_JAR}" ]]; then
    echo "Aggregation job jar not found: ${AGGREGATION_JOB_JAR}" >&2
    echo "Build Flink jobs in STEP 8 before submitting." >&2
    exit 1
  fi
  docker compose cp "${AGGREGATION_JOB_JAR}" flink-jobmanager:/tmp/aggregation-job.jar
else
  docker compose exec -T flink-jobmanager cp /tmp/validation-job.jar /tmp/aggregation-job.jar
fi

docker compose exec -T flink-jobmanager flink run \
  -m "${FLINK_JOBMANAGER}" \
  -c "${AGGREGATION_JOB_CLASS}" \
  /tmp/aggregation-job.jar \
  --bootstrap.servers "${KAFKA_BOOTSTRAP_SERVERS}" \
  --clean.topic clean-user-events \
  --late.topic late-events-dlq \
  --feature.user.topic feature-user-updates \
  --feature.product.topic feature-product-updates \
  --feature.category.topic feature-category-updates \
  --group.id feature-aggregation-job \
  --redis.host "${REDIS_HOST}" \
  --redis.port "${REDIS_PORT}" \
  --jdbc.url "${POSTGRES_JDBC_URL}" \
  --jdbc.user "${POSTGRES_USER}" \
  --jdbc.password "${POSTGRES_PASSWORD}"
