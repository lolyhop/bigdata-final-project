"""Prepare the raw Hive parquet for ML: restore dates, add features, filter labels."""

import math
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ml_utils import read_hive

load_dotenv()
HDFS_DATA_DIR = os.environ.get("HDFS_DATA_DIR")
ML_PREPARED_RAW_PATH = os.environ.get(
    "ML_PREPARED_RAW_PATH",
)


def build_spark():
    """Create and return a Spark session connected to YARN.

    Returns:
        Active SparkSession.
    """
    return (
        SparkSession.builder.appName("team25 - prepare raw ml dataset")
        .master("yarn")
        .getOrCreate()
    )


def restore_datetime_columns(df):
    """Cast epoch-millisecond integer columns to proper date columns.

    Args:
        df: Spark DataFrame that may contain ``issue_d`` and ``earliest_cr_line``
            as integer epoch-millisecond columns (Sqoop artefact).

    Returns:
        DataFrame with those columns cast to ``date`` type.
    """
    issue_type = dict(df.dtypes).get("issue_d")
    earliest_type = dict(df.dtypes).get("earliest_cr_line")

    if issue_type not in ("date", "timestamp"):
        df = df.withColumn(
            "issue_d", F.to_date((F.col("issue_d") / F.lit(1000)).cast("timestamp"))
        )

    if earliest_type not in ("date", "timestamp"):
        df = df.withColumn(
            "earliest_cr_line",
            F.to_date((F.col("earliest_cr_line") / F.lit(1000)).cast("timestamp")),
        )

    return df


def add_datetime_features(df):
    """Derive cyclic month and credit-history features from date columns.

    Args:
        df: Spark DataFrame with ``issue_d`` and ``earliest_cr_line`` date columns.

    Returns:
        DataFrame with added ``issue_year``, ``issue_month``, ``issue_month_sin``,
        ``issue_month_cos``, and ``credit_history_months`` columns.
    """
    df = df.withColumn("issue_year", F.year("issue_d"))
    df = df.withColumn("issue_month", F.month("issue_d"))

    df = df.withColumn(
        "issue_month_sin",
        F.sin(F.lit(2.0 * math.pi) * F.col("issue_month") / F.lit(12.0)),
    )
    df = df.withColumn(
        "issue_month_cos",
        F.cos(F.lit(2.0 * math.pi) * F.col("issue_month") / F.lit(12.0)),
    )

    df = df.withColumn(
        "credit_history_months",
        F.months_between(F.col("issue_d"), F.col("earliest_cr_line")),
    )

    return df


def select_and_prepare_rows(df):
    """Filter to binary-label rows and select only the model-relevant columns.

    Args:
        df: Spark DataFrame with the full feature set after datetime engineering.

    Returns:
        Filtered DataFrame with a ``label`` column and only the selected features.
    """
    df = df.filter(F.col("loan_status").isin("Fully Paid", "Charged Off"))

    df = df.withColumn(
        "label",
        F.when(F.col("loan_status") == "Charged Off", F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    selected_columns = [
        "id",
        "loan_status",
        "label",
        # numeric
        "loan_amnt",
        "term",
        "int_rate",
        "installment",
        "annual_inc",
        "dti",
        "delinq_2yrs",
        "inq_last_6mths",
        "open_acc",
        "pub_rec",
        "revol_bal",
        "revol_util",
        "total_acc",
        "fico_range_low",
        "fico_range_high",
        # categorical
        "grade",
        "sub_grade",
        "emp_length",
        "home_ownership",
        "verification_status",
        "purpose",
        "application_type",
        # engineered datetime features
        "issue_year",
        "issue_month",
        "issue_month_sin",
        "issue_month_cos",
        "credit_history_months",
    ]

    return df.select(*selected_columns)


def main():
    """Orchestrate dataset preparation: restore dates, engineer features, save parquet."""
    spark = build_spark()

    df = read_hive(spark, HDFS_DATA_DIR)

    print("\n=== SOURCE SCHEMA ===")
    df.printSchema()

    df = restore_datetime_columns(df)
    df = add_datetime_features(df)
    df = select_and_prepare_rows(df)

    print("\n=== BINARY LABEL DISTRIBUTION ===")
    df.groupBy("label").count().orderBy("label").show()

    print("\n=== PREPARED RAW SCHEMA ===")
    df.printSchema()

    print("\nSaving prepared raw dataset to:", ML_PREPARED_RAW_PATH)
    df.write.mode("overwrite").parquet(ML_PREPARED_RAW_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
