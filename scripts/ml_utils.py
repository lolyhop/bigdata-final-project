"""Shared ML utility functions: evaluation metrics, CSV/JSON savers, param formatters."""

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F


def evaluate_binary_predictions(
    predictions,
    label_col="label",
    prediction_col="prediction",
    raw_prediction_col="rawPrediction",
):
    """Compute binary classification metrics from a predictions DataFrame.

    Args:
        predictions: Spark DataFrame with ``label``, ``prediction``,
            and ``rawPrediction`` columns.
        label_col: Name of the true-label column.
        prediction_col: Name of the predicted-label column.
        raw_prediction_col: Name of the raw-prediction column used for AUC.

    Returns:
        Dict with keys ``accuracy``, ``precision``, ``recall``, ``f1``,
        ``areaUnderROC``, ``areaUnderPR``, ``tp``, ``tn``, ``fp``, ``fn``.
    """
    tp = predictions.filter(
        (F.col(label_col) == 1.0) & (F.col(prediction_col) == 1.0)
    ).count()
    tn = predictions.filter(
        (F.col(label_col) == 0.0) & (F.col(prediction_col) == 0.0)
    ).count()
    fp = predictions.filter(
        (F.col(label_col) == 0.0) & (F.col(prediction_col) == 1.0)
    ).count()
    fn = predictions.filter(
        (F.col(label_col) == 1.0) & (F.col(prediction_col) == 0.0)
    ).count()

    total = tp + tn + fp + fn

    accuracy = (tp + tn) / float(total) if total > 0 else 0.0
    precision = tp / float(tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / float(tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (
        2.0 * precision * recall / float(precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=raw_prediction_col,
        metricName="areaUnderROC",
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=raw_prediction_col,
        metricName="areaUnderPR",
    )

    area_under_roc = roc_evaluator.evaluate(predictions)
    area_under_pr = pr_evaluator.evaluate(predictions)

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "areaUnderROC": area_under_roc,
        "areaUnderPR": area_under_pr,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }

def read_hive(spark, path):
    """
    Reads a Spark DataFrame from a Hive table using the provided path.
    """
    try:
        return spark.table(path)
    except Exception:
        return spark.read.parquet(path)

def save_metrics_csv(spark, model_name, metrics, best_params, output_path):
    """Write a single-row metrics CSV for *model_name* to *output_path*.

    Args:
        spark: Active SparkSession.
        model_name: Display name of the model (written to the ``model`` column).
        metrics: Dict returned by :func:`evaluate_binary_predictions`.
        best_params: Human-readable best hyperparameter string (may be empty).
        output_path: HDFS destination path.
    """
    df = spark.createDataFrame(
        [
            (
                model_name,
                metrics["accuracy"],
                metrics["precision"],
                metrics["recall"],
                metrics["f1"],
                metrics["areaUnderROC"],
                metrics["areaUnderPR"],
                metrics["tp"],
                metrics["tn"],
                metrics["fp"],
                metrics["fn"],
                best_params,
            )
        ],
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

    (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path))


def save_predictions(predictions, output_path):
    """Write ``label`` and ``prediction`` columns from *predictions* to *output_path*.

    Args:
        predictions: Spark DataFrame containing ``label`` and ``prediction`` columns.
        output_path: HDFS destination path for the CSV file.
    """
    (
        predictions.select("label", "prediction")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )


def pipeline_model_params_to_string(pipeline_model):
    """Serialize all stage hyperparameters of a fitted pipeline model to a string.

    Args:
        pipeline_model: Fitted Spark PipelineModel.

    Returns:
        Semicolon-delimited string of ``stage_uid__param=value`` pairs, sorted.
    """
    pairs = []
    for stage in pipeline_model.stages:
        for param, value in stage.extractParamMap().items():
            pairs.append(f"{stage.uid}__{param.name}={value}")
    return "; ".join(sorted(pairs))


def param_map_to_string(model):
    """Serialize the hyperparameters of a fitted model to a semicolon-delimited string.

    Args:
        model: Fitted Spark ML model with an ``extractParamMap`` method.

    Returns:
        Semicolon-delimited string of ``param=value`` pairs, sorted alphabetically.
    """
    param_map = model.extractParamMap()
    pairs = []

    for param, value in param_map.items():
        pairs.append(f"{param.name}={value}")

    pairs = sorted(pairs)
    return "; ".join(pairs)


def build_prediction_distribution_df(predictions, model_name):
    """Return a DataFrame counting prediction/label combinations for *model_name*.

    Args:
        predictions: Spark DataFrame with ``label`` and ``prediction`` columns.
        model_name: Model identifier written to the ``model`` column.

    Returns:
        Spark DataFrame with columns ``model``, ``label``, ``prediction``, ``count``.
    """
    return (
        predictions.groupBy("label", "prediction")
        .count()
        .withColumn("model", F.lit(model_name))
        .select("model", "label", "prediction", "count")
    )
