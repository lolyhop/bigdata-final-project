#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="$PROJECT_ROOT/logs"
OUTPUT_DIR="$PROJECT_ROOT/output"
DASHBOARD_DIR="$PROJECT_ROOT/output/dashboard"
DATA_DIR="$PROJECT_ROOT/data"
MODELS_DIR="$PROJECT_ROOT/models"

mkdir -p "$LOG_DIR" "$OUTPUT_DIR" "$DASHBOARD_DIR" "$DATA_DIR" "$MODELS_DIR"

set -a
source "$PROJECT_ROOT/.env"
set +a

export PYSPARK_PYTHON=python3

SPARK_SUBMIT="/usr/bin/spark-submit"
SPARK_COMMON_ARGS=(
  --master yarn
  --deploy-mode client
)

SPARK_TRAIN_ARGS=(
  --master yarn
  --deploy-mode client
  --driver-memory 4g
  --executor-memory 4g
)

run_spark_job() {
  local name="$1"
  local script_path="$2"
  shift 2

  echo
  echo "Running ${name}"
  echo "Script: ${script_path}"
  echo "Log: ${LOG_DIR}/${name}.log"

  "$SPARK_SUBMIT" "$@" "$script_path" 2>&1 | tee "${LOG_DIR}/${name}.log"
}

export_csv_from_hdfs() {
  local hdfs_path="$1"
  local local_path="$2"

  echo "Exporting CSV: ${hdfs_path} -> ${local_path}"
  hdfs dfs -cat "${hdfs_path}"/part-*.csv > "${local_path}"
}

export_json_from_hdfs() {
  local hdfs_path="$1"
  local local_path="$2"

  echo "Exporting JSON: ${hdfs_path} -> ${local_path}"
  hdfs dfs -cat "${hdfs_path}"/part-*.json > "${local_path}"
}

export_model_from_hdfs() {
  local hdfs_path="$1"
  local local_path="$2"

  echo "Exporting model: ${hdfs_path} -> ${local_path}"
  rm -rf "${local_path}"
  hdfs dfs -get -f "${hdfs_path}" "${local_path}"
}

echo "Stage III started"
echo "Project root: ${PROJECT_ROOT}"

run_spark_job \
  "prepare_raw_ml_dataset" \
  "scripts/prepare_raw_ml_dataset.py" \
  "${SPARK_COMMON_ARGS[@]}"

run_spark_job \
  "build_train_test_datasets" \
  "scripts/build_train_test_datasets.py" \
  "${SPARK_COMMON_ARGS[@]}"

run_spark_job \
  "train_baseline_models" \
  "scripts/train_baseline_models.py" \
  "${SPARK_TRAIN_ARGS[@]}"

run_spark_job \
  "train_tuned_models" \
  "scripts/train_tuned_models.py" \
  "${SPARK_TRAIN_ARGS[@]}"

echo
echo "Exporting Stage III artifacts from HDFS"

export_csv_from_hdfs "$ML_SPLIT_SUMMARY_PATH" "$OUTPUT_DIR/split_summary.csv"
export_csv_from_hdfs "$ML_FEATURE_INFO_PATH" "$OUTPUT_DIR/feature_info.csv"
export_csv_from_hdfs "$ML_LABEL_DISTRIBUTION_PATH" "$OUTPUT_DIR/label_distribution.csv"

export_csv_from_hdfs "$ML_BASELINE_EVALUATION_PATH" "$OUTPUT_DIR/baseline_evaluation.csv"
export_csv_from_hdfs "$ML_BASELINE_RF_METRICS_PATH" "$OUTPUT_DIR/baseline_rf_metrics.csv"
export_csv_from_hdfs "$ML_BASELINE_SVM_METRICS_PATH" "$OUTPUT_DIR/baseline_svm_metrics.csv"
export_csv_from_hdfs "$ML_BASELINE_NB_METRICS_PATH" "$OUTPUT_DIR/baseline_nb_metrics.csv"

export_csv_from_hdfs "$ML_EVALUATION_PATH" "$OUTPUT_DIR/evaluation.csv"
export_csv_from_hdfs "$ML_MODEL1_METRICS_PATH" "$OUTPUT_DIR/model1_metrics.csv"
export_csv_from_hdfs "$ML_MODEL2_METRICS_PATH" "$OUTPUT_DIR/model2_metrics.csv"
export_csv_from_hdfs "$ML_MODEL3_METRICS_PATH" "$OUTPUT_DIR/model3_metrics.csv"

export_csv_from_hdfs "$ML_PREDICTION_DISTRIBUTION_PATH" "$OUTPUT_DIR/prediction_distribution.csv"

export_csv_from_hdfs "$ML_MODEL1_PRED_PATH" "$OUTPUT_DIR/model1_predictions.csv"
export_csv_from_hdfs "$ML_MODEL2_PRED_PATH" "$OUTPUT_DIR/model2_predictions.csv"
export_csv_from_hdfs "$ML_MODEL3_PRED_PATH" "$OUTPUT_DIR/model3_predictions.csv"

export_json_from_hdfs "$ML_TRAIN_JSON_PATH" "$DATA_DIR/train.json"
export_json_from_hdfs "$ML_TEST_JSON_PATH" "$DATA_DIR/test.json"

export_model_from_hdfs "$ML_MODEL1_PATH" "$MODELS_DIR/model1"
export_model_from_hdfs "$ML_MODEL2_PATH" "$MODELS_DIR/model2"
export_model_from_hdfs "$ML_MODEL3_PATH" "$MODELS_DIR/model3"

echo
echo "Preparing dashboard CSV package"

cp "$OUTPUT_DIR/split_summary.csv" "$DASHBOARD_DIR/split_summary.csv"
cp "$OUTPUT_DIR/feature_info.csv" "$DASHBOARD_DIR/feature_info.csv"
cp "$OUTPUT_DIR/label_distribution.csv" "$DASHBOARD_DIR/label_distribution.csv"
cp "$OUTPUT_DIR/baseline_evaluation.csv" "$DASHBOARD_DIR/baseline_evaluation.csv"
cp "$OUTPUT_DIR/evaluation.csv" "$DASHBOARD_DIR/evaluation.csv"
cp "$OUTPUT_DIR/prediction_distribution.csv" "$DASHBOARD_DIR/prediction_distribution.csv"

echo
echo "Dashboard CSV files:"
ls -lh "$DASHBOARD_DIR"

echo
echo "Stage III finished successfully"
echo "Main outputs:"
echo "- output/evaluation.csv"
echo "- output/baseline_evaluation.csv"
echo "- output/prediction_distribution.csv"
echo "- output/dashboard/"
echo "- data/train.json"
echo "- data/test.json"
echo "- models/model1"
echo "- models/model2"
echo "- models/model3"
