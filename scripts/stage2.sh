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

if [ -n "${HADOOP_CONF_DIR:-}" ]; then
  export HADOOP_CONF_DIR
fi

python3 scripts/load_hive.py

mkdir -p output

echo "Running EDA queries ..."
for q in 1 2 3 4 5 6; do
  echo "  EDA query ${q} ..."
  python3 "scripts/eda/q${q}.py"
done

echo "Stage II finished successfully."