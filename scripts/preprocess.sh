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

if [ -z "${DATASET_INPUT_PATH:-}" ]; then
  echo "DATASET_INPUT_PATH must be set in .env. See .env.example." >&2
  exit 1
fi
if [ -z "${DATASET_FILTERED_PATH:-}" ]; then
  echo "DATASET_FILTERED_PATH must be set in .env. See .env.example." >&2
  exit 1
fi

python scripts/filter_dataset_for_pg.py
