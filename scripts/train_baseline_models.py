"""Train and evaluate Random Forest, Linear SVC, and Naive Bayes baseline models."""

import os

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, NaiveBayes, RandomForestClassifier
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql import SparkSession

from ml_utils import evaluate_binary_predictions, save_metrics_csv, save_predictions

load_dotenv()
ML_TRAIN_ENCODED_PATH = os.environ.get("ML_TRAIN_ENCODED_PATH")
ML_TEST_ENCODED_PATH = os.environ.get("ML_TEST_ENCODED_PATH")

ML_BASELINE_RF_PRED_PATH = os.environ.get("ML_BASELINE_RF_PRED_PATH")
ML_BASELINE_SVM_PRED_PATH = os.environ.get("ML_BASELINE_SVM_PRED_PATH")
ML_BASELINE_NB_PRED_PATH = os.environ.get("ML_BASELINE_NB_PRED_PATH")

ML_BASELINE_RF_METRICS_PATH = os.environ.get("ML_BASELINE_RF_METRICS_PATH")
ML_BASELINE_SVM_METRICS_PATH = os.environ.get("ML_BASELINE_SVM_METRICS_PATH")
ML_BASELINE_NB_METRICS_PATH = os.environ.get("ML_BASELINE_NB_METRICS_PATH")

ML_BASELINE_EVALUATION_PATH = os.environ.get("ML_BASELINE_EVALUATION_PATH")


def build_spark():
    """Create and return a Spark session connected to YARN.

    Returns:
        Active SparkSession.
    """
    return (
        SparkSession.builder.appName("team25 - train baseline models")
        .master("yarn")
        .getOrCreate()
    )


def train_random_forest(train_df, test_df):
    """Fit a baseline Random Forest classifier and evaluate it on *test_df*.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for evaluation.

    Returns:
        Tuple of ``(model, predictions, metrics)``.
    """
    model = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=10,
        maxDepth=3,
        seed=42,
    ).fit(train_df)

    predictions = model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return model, predictions, metrics


def train_linear_svc(train_df, test_df):
    """Fit a baseline Linear SVC and evaluate it on *test_df*.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for evaluation.

    Returns:
        Tuple of ``(model, predictions, metrics)``.
    """
    model = LinearSVC(
        labelCol="label",
        featuresCol="features",
        regParam=0.1,
        maxIter=50,
    ).fit(train_df)

    predictions = model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return model, predictions, metrics


def train_naive_bayes(train_df, test_df):
    """Fit a MinMaxScaler → Naive Bayes pipeline and evaluate it on *test_df*.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for evaluation.

    Returns:
        Tuple of ``(pipeline_model, predictions, metrics)``.
    """
    scaler = MinMaxScaler(
        inputCol="features",
        outputCol="features_scaled",
    )

    nb = NaiveBayes(
        labelCol="label",
        featuresCol="features_scaled",
        smoothing=1.0,
        modelType="multinomial",
    )

    pipeline = Pipeline(stages=[scaler, nb])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return model, predictions, metrics


def main():
    """Train all three baseline models and save predictions and evaluation summary."""
    spark = build_spark()

    print("Reading encoded train dataset from:", ML_TRAIN_ENCODED_PATH)
    train_df = spark.read.parquet(ML_TRAIN_ENCODED_PATH)

    print("Reading encoded test dataset from:", ML_TEST_ENCODED_PATH)
    test_df = spark.read.parquet(ML_TEST_ENCODED_PATH)

    print("\nTrain rows:", train_df.count())
    print("Test rows:", test_df.count())

    print("\nTraining Random Forest baseline")
    _, rf_predictions, rf_metrics = train_random_forest(train_df, test_df)
    print("Random Forest metrics:", rf_metrics)

    print("Saving Random Forest predictions to:", ML_BASELINE_RF_PRED_PATH)
    save_predictions(rf_predictions, ML_BASELINE_RF_PRED_PATH)

    print("Saving Random Forest metrics to:", ML_BASELINE_RF_METRICS_PATH)
    save_metrics_csv(
        spark,
        "RandomForest",
        rf_metrics,
        "",
        ML_BASELINE_RF_METRICS_PATH,
    )

    print("\nTraining Linear SVC baseline")
    _, svm_predictions, svm_metrics = train_linear_svc(train_df, test_df)
    print("Linear SVC metrics:", svm_metrics)

    print("Saving Linear SVC predictions to:", ML_BASELINE_SVM_PRED_PATH)
    save_predictions(svm_predictions, ML_BASELINE_SVM_PRED_PATH)

    print("Saving Linear SVC metrics to:", ML_BASELINE_SVM_METRICS_PATH)
    save_metrics_csv(spark, "LinearSVC", svm_metrics, "", ML_BASELINE_SVM_METRICS_PATH)

    print("\nTraining Naive Bayes baseline")
    _, nb_predictions, nb_metrics = train_naive_bayes(train_df, test_df)
    print("Naive Bayes metrics:", nb_metrics)

    print("Saving Naive Bayes predictions to:", ML_BASELINE_NB_PRED_PATH)
    save_predictions(nb_predictions, ML_BASELINE_NB_PRED_PATH)

    print("Saving Naive Bayes metrics to:", ML_BASELINE_NB_METRICS_PATH)
    save_metrics_csv(spark, "NaiveBayes", nb_metrics, "", ML_BASELINE_NB_METRICS_PATH)

    evaluation_rows = [
        (
            "RandomForest",
            rf_metrics["accuracy"],
            rf_metrics["precision"],
            rf_metrics["recall"],
            rf_metrics["f1"],
            rf_metrics["areaUnderROC"],
            rf_metrics["areaUnderPR"],
            rf_metrics["tp"],
            rf_metrics["tn"],
            rf_metrics["fp"],
            rf_metrics["fn"],
        ),
        (
            "LinearSVC",
            svm_metrics["accuracy"],
            svm_metrics["precision"],
            svm_metrics["recall"],
            svm_metrics["f1"],
            svm_metrics["areaUnderROC"],
            svm_metrics["areaUnderPR"],
            svm_metrics["tp"],
            svm_metrics["tn"],
            svm_metrics["fp"],
            svm_metrics["fn"],
        ),
        (
            "NaiveBayes",
            nb_metrics["accuracy"],
            nb_metrics["precision"],
            nb_metrics["recall"],
            nb_metrics["f1"],
            nb_metrics["areaUnderROC"],
            nb_metrics["areaUnderPR"],
            nb_metrics["tp"],
            nb_metrics["tn"],
            nb_metrics["fp"],
            nb_metrics["fn"],
        ),
    ]

    evaluation_df = spark.createDataFrame(
        evaluation_rows,
        [
            "model",
            "accuracy",
            "precision",
            "recall",
            "f1",
            "areaUnderROC",
            "areaUnderPR",
            "tp",
            "tn",
            "fp",
            "fn",
        ],
    )

    print("\nBaseline evaluation summary")
    evaluation_df.show(truncate=False)

    print("Saving baseline evaluation summary to:", ML_BASELINE_EVALUATION_PATH)
    (
        evaluation_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(ML_BASELINE_EVALUATION_PATH)
    )

    print("\nBaseline models are evaluated")
    spark.stop()


if __name__ == "__main__":
    main()
