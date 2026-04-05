#!/bin/bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

if [ -z "${DATASET_FILTERED_PATH:-}" ]; then
  echo "DATASET_FILTERED_PATH must be set in .env. See .env.example." >&2
  exit 1
fi
if [ ! -f "$DATASET_FILTERED_PATH" ]; then
  echo "Missing file: ${DATASET_FILTERED_PATH}. Run preprocess first." >&2
  exit 1
fi

python3 scripts/load_postgres.py

if ! command -v sqoop >/dev/null 2>&1; then
  echo "sqoop not found on PATH. Run stage 1 on a cluster edge node or VM with Sqoop installed." >&2
  exit 1
fi

if [ -z "${SQOOP_TARGET_DIR:-}" ]; then
  echo "SQOOP_TARGET_DIR must be set in .env. See .env.example." >&2
  exit 1
fi
if [ -z "${HADOOP_CONF_DIR:-}" ]; then
  echo "HADOOP_CONF_DIR must be set in .env. See .env.example." >&2
  exit 1
fi
if [ -z "${TABLE_NAME:-}" ]; then
  echo "TABLE_NAME must be set in .env (same value load_postgres.py and Sqoop use). See .env.example." >&2
  exit 1
fi
export HADOOP_CONF_DIR

SQOOP_NUM_MAPPERS="${SQOOP_NUM_MAPPERS:-4}"
JDBC_URL="jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}"

echo "Starting Sqoop import of table ${TABLE_NAME} to ${SQOOP_TARGET_DIR} (${SQOOP_NUM_MAPPERS} mappers)."

sqoop import \
  --connect "${JDBC_URL}" \
  --username "${PGUSER}" \
  --password "${PGPASSWORD}" \
  --table "${TABLE_NAME}" \
  --target-dir "${SQOOP_TARGET_DIR}" \
  --delete-target-dir \
  --split-by id \
  -m "${SQOOP_NUM_MAPPERS}"

echo "Sqoop import finished."
