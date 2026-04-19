from pyspark.sql import functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def evaluate_binary_predictions(
    predictions,
    label_col="label",
    prediction_col="prediction",
    raw_prediction_col="rawPrediction",
):
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


def save_metrics_csv(spark, model_name, metrics, best_params, output_path):
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
    cols = ["label", "prediction"]
    if "rawPrediction" in predictions.columns:
        cols.append("rawPrediction")
    if "probability" in predictions.columns:
        cols.append("probability")

    (
        predictions.select(*cols)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )


def param_map_to_string(model):
    param_map = model.extractParamMap()
    pairs = []

    for param, value in param_map.items():
        pairs.append("{}={}".format(param.name, value))

    pairs = sorted(pairs)
    return "; ".join(pairs)


def build_prediction_distribution_df(predictions, model_name):

    return (
        predictions.groupBy("label", "prediction")
        .count()
        .withColumn("model", F.lit(model_name))
        .select("model", "label", "prediction", "count")
    )
