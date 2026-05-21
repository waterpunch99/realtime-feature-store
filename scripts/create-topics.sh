#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

cd "${ROOT_DIR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic raw-user-events \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic clean-user-events \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic invalid-user-events-dlq \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic late-events-dlq \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic feature-user-updates \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic feature-product-updates \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic feature-category-updates \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker compose exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list

