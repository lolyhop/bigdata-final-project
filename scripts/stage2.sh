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

mkdir -p output

python3 scripts/load_hive.py

echo "Running business EDA queries ..."
for q in 1 2 3 4 5 6; do
  echo "  EDA query q${q} ..."
  python3 "scripts/eda/q${q}.py"
done

echo "Running data quality EDA queries ..."
for hql_path in sql/data_quality/*.hql; do
  hql_file="$(basename "$hql_path")"
  query_name="${hql_file%.hql}"
  result_table="${query_name}_results"
  output_file="${query_name}.csv"

  echo "  Data quality query ${query_name} ..."
  python3 scripts/eda/run_hql_export.py \
    --hql "data_quality/${hql_file}" \
    --table "$result_table" \
    --output "$output_file"
done

echo "Stage II finished successfully."