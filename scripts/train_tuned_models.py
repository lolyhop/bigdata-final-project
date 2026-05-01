"""Hyperparameter-tune Random Forest, Linear SVC, and Naive Bayes via cross-validation."""

import os

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

from ml_utils import (
    build_prediction_distribution_df,
    evaluate_binary_predictions,
    param_map_to_string,
    pipeline_model_params_to_string,
    save_metrics_csv,
    save_predictions,
)

load_dotenv()
ML_TRAIN_ENCODED_PATH = os.environ.get("ML_TRAIN_ENCODED_PATH")
ML_TEST_ENCODED_PATH = os.environ.get("ML_TEST_ENCODED_PATH")

ML_MODEL1_PATH = os.environ.get("ML_MODEL1_PATH")
ML_MODEL2_PATH = os.environ.get("ML_MODEL2_PATH")
ML_MODEL3_PATH = os.environ.get("ML_MODEL3_PATH")

ML_MODEL1_PRED_PATH = os.environ.get("ML_MODEL1_PRED_PATH")
ML_MODEL2_PRED_PATH = os.environ.get("ML_MODEL2_PRED_PATH")
ML_MODEL3_PRED_PATH = os.environ.get("ML_MODEL3_PRED_PATH")

ML_MODEL1_METRICS_PATH = os.environ.get("ML_MODEL1_METRICS_PATH")
ML_MODEL2_METRICS_PATH = os.environ.get("ML_MODEL2_METRICS_PATH")
ML_MODEL3_METRICS_PATH = os.environ.get("ML_MODEL3_METRICS_PATH")
ML_PREDICTION_DISTRIBUTION_PATH = os.environ.get("ML_PREDICTION_DISTRIBUTION_PATH")

ML_EVALUATION_PATH = os.environ.get("ML_EVALUATION_PATH")

ML_CV_FOLDS = int(os.environ.get("ML_CV_FOLDS"))
ML_CV_PARALLELISM = int(os.environ.get("ML_CV_PARALLELISM"))
ML_OPTIMIZATION_METRIC = os.environ.get("ML_OPTIMIZATION_METRIC")


def build_spark():
    """Create and return a Spark session connected to YARN.

    Returns:
        Active SparkSession.
    """
    return (
        SparkSession.builder.appName("team25 - train tuned models")
        .master("yarn")
        .getOrCreate()
    )


def get_train_dataset(spark):
    """Read the encoded training parquet from HDFS.

    Args:
        spark: Active SparkSession.

    Returns:
        Training Spark DataFrame.
    """
    print("Reading train dataset from:", ML_TRAIN_ENCODED_PATH)
    return spark.read.parquet(ML_TRAIN_ENCODED_PATH)


def get_test_dataset(spark):
    """Read the encoded test parquet from HDFS.

    Args:
        spark: Active SparkSession.

    Returns:
        Test Spark DataFrame.
    """
    print("Reading encoded test dataset from:", ML_TEST_ENCODED_PATH)
    return spark.read.parquet(ML_TEST_ENCODED_PATH)


def tune_random_forest(train_df, test_df):
    """Cross-validate a Random Forest classifier and evaluate the best model.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for final evaluation.

    Returns:
        Tuple of ``(cv_model, predictions, metrics)``.
    """
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        seed=42,
    )

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [10, 20, 30])
        .addGrid(rf.maxDepth, [3, 5, 7])
        .addGrid(rf.minInstancesPerNode, [5, 10, 20])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName=ML_OPTIMIZATION_METRIC,
    )

    cv = CrossValidator(
        estimator=rf,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=ML_CV_FOLDS,
        parallelism=ML_CV_PARALLELISM,
    )

    cv_model = cv.fit(train_df)
    predictions = cv_model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return cv_model, predictions, metrics


def tune_linear_svc(train_df, test_df):
    """Cross-validate a Linear SVC and evaluate the best model.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for final evaluation.

    Returns:
        Tuple of ``(cv_model, predictions, metrics)``.
    """
    svc = LinearSVC(
        labelCol="label",
        featuresCol="features",
    )

    param_grid = (
        ParamGridBuilder()
        .addGrid(svc.regParam, [0.001, 0.01, 0.1])
        .addGrid(svc.tol, [1e-6, 1e-5, 1e-4])
        .addGrid(svc.aggregationDepth, [2, 4, 8])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName=ML_OPTIMIZATION_METRIC,
    )

    cv = CrossValidator(
        estimator=svc,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=ML_CV_FOLDS,
        parallelism=ML_CV_PARALLELISM,
    )

    cv_model = cv.fit(train_df)
    predictions = cv_model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return cv_model, predictions, metrics


def tune_naive_bayes(train_df, test_df):
    """Cross-validate a MinMaxScaler → Naive Bayes pipeline and evaluate the best model.

    Args:
        train_df: Encoded training DataFrame with ``features`` and ``label`` columns.
        test_df: Encoded test DataFrame used for final evaluation.

    Returns:
        Tuple of ``(cv_model, predictions, metrics)``.
    """
    scaler = MinMaxScaler(
        inputCol="features",
        outputCol="features_scaled",
    )

    nb = NaiveBayes(
        labelCol="label",
        featuresCol="features_scaled",
        modelType="multinomial",
    )

    pipeline = Pipeline(stages=[scaler, nb])

    param_grid = (
        ParamGridBuilder()
        .addGrid(scaler.max, [1.0, 2.0, 5.0])
        .addGrid(nb.smoothing, [0.5, 1.0, 1.5])
        .addGrid(nb.thresholds, [[1.0, 1.0], [1.0, 2.0], [2.0, 1.0]])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName=ML_OPTIMIZATION_METRIC,
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=ML_CV_FOLDS,
        parallelism=ML_CV_PARALLELISM,
    )

    cv_model = cv.fit(train_df)
    predictions = cv_model.transform(test_df)
    metrics = evaluate_binary_predictions(predictions)

    return cv_model, predictions, metrics


def main():
    """Tune all three models, evaluate them, and save models, predictions, and metrics."""
    spark = build_spark()

    train_df = get_train_dataset(spark)
    test_df = get_test_dataset(spark)

    print("\nTrain rows:", train_df.count())
    print("Test rows:", test_df.count())
    print("Optimization metric for CV:", ML_OPTIMIZATION_METRIC)
    print("CV folds:", ML_CV_FOLDS)

    print("\nTraining tuned Random Forest")
    rf_cv_model, rf_predictions, rf_metrics = tune_random_forest(train_df, test_df)
    rf_best_params = param_map_to_string(rf_cv_model.bestModel)
    print("Random Forest metrics:", rf_metrics)

    print("Saving Random Forest best model to:", ML_MODEL1_PATH)
    rf_cv_model.bestModel.write().overwrite().save(ML_MODEL1_PATH)

    print("Saving Random Forest predictions to:", ML_MODEL1_PRED_PATH)
    save_predictions(rf_predictions, ML_MODEL1_PRED_PATH)

    print("Saving Random Forest metrics to:", ML_MODEL1_METRICS_PATH)
    save_metrics_csv(
        spark,
        "RandomForest",
        rf_metrics,
        rf_best_params,
        ML_MODEL1_METRICS_PATH,
    )

    print("\nTraining tuned Linear SVC")
    svm_cv_model, svm_predictions, svm_metrics = tune_linear_svc(train_df, test_df)
    svm_best_params = param_map_to_string(svm_cv_model.bestModel)
    print("Linear SVC metrics:", svm_metrics)

    print("Saving Linear SVC best model to:", ML_MODEL2_PATH)
    svm_cv_model.bestModel.write().overwrite().save(ML_MODEL2_PATH)

    print("Saving Linear SVC predictions to:", ML_MODEL2_PRED_PATH)
    save_predictions(svm_predictions, ML_MODEL2_PRED_PATH)

    print("Saving Linear SVC metrics to:", ML_MODEL2_METRICS_PATH)
    save_metrics_csv(
        spark,
        "LinearSVC",
        svm_metrics,
        svm_best_params,
        ML_MODEL2_METRICS_PATH,
    )

    print("\nTraining tuned Naive Bayes")
    nb_cv_model, nb_predictions, nb_metrics = tune_naive_bayes(train_df, test_df)
    nb_best_params = pipeline_model_params_to_string(nb_cv_model.bestModel)
    print("Naive Bayes metrics:", nb_metrics)

    print("Saving Naive Bayes best model to:", ML_MODEL3_PATH)
    nb_cv_model.bestModel.write().overwrite().save(ML_MODEL3_PATH)

    print("Saving Naive Bayes predictions to:", ML_MODEL3_PRED_PATH)
    save_predictions(nb_predictions, ML_MODEL3_PRED_PATH)

    print("Saving Naive Bayes metrics to:", ML_MODEL3_METRICS_PATH)
    save_metrics_csv(
        spark,
        "NaiveBayes",
        nb_metrics,
        nb_best_params,
        ML_MODEL3_METRICS_PATH,
    )

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
            rf_best_params,
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
            svm_best_params,
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
            nb_best_params,
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
            "best_params",
        ],
    )

    print("\nTuned model evaluation summary")
    evaluation_df.show(truncate=False)

    print("Saving evaluation summary to:", ML_EVALUATION_PATH)
    (
        evaluation_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(ML_EVALUATION_PATH)
    )

    rf_pred_dist = build_prediction_distribution_df(rf_predictions, "RandomForest")
    svm_pred_dist = build_prediction_distribution_df(svm_predictions, "LinearSVC")
    nb_pred_dist = build_prediction_distribution_df(nb_predictions, "NaiveBayes")

    prediction_distribution_df = rf_pred_dist.unionByName(svm_pred_dist).unionByName(
        nb_pred_dist
    )

    print("Saving prediction distribution summary to:", ML_PREDICTION_DISTRIBUTION_PATH)
    (
        prediction_distribution_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(ML_PREDICTION_DISTRIBUTION_PATH)
    )

    print("\nTuned models are trained and evaluated")
    spark.stop()


if __name__ == "__main__":
    main()
