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
if [ -z "${DATASET_YANDEX_DISK_SHARE_URL:-}" ]; then
  echo "DATASET_YANDEX_DISK_SHARE_URL must be set in .env for download." >&2
  exit 1
fi

python3 scripts/download_from_yandex_disk.py \
  "${DATASET_YANDEX_DISK_SHARE_URL}" \
  "${DATASET_INPUT_PATH}.zip"

DATASET_DIR="$(dirname "${DATASET_INPUT_PATH}")"
mkdir -p "${DATASET_DIR}"

# List members from the zip only (avoids picking other .csv files already in DATASET_DIR).
FIRST_MEMBER="$(unzip -Z1 "${DATASET_INPUT_PATH}.zip" | grep -iE '\.csv$' | head -n 1 || true)"
if [ -z "${FIRST_MEMBER}" ]; then
  echo "No .csv member found inside ${DATASET_INPUT_PATH}.zip" >&2
  exit 1
fi
# -j drops archive path so the file lands in DATASET_DIR under its basename.
unzip -q -o -j "${DATASET_INPUT_PATH}.zip" "${FIRST_MEMBER}" -d "${DATASET_DIR}"
mv "${DATASET_DIR}/$(basename "${FIRST_MEMBER}")" "${DATASET_INPUT_PATH}"

python3 scripts/filter_dataset_for_pg.py
