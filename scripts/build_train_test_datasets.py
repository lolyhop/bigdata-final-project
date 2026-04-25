import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder


ML_PREPARED_RAW_PATH = os.environ.get(
    "ML_PREPARED_RAW_PATH",
)

ML_TRAIN_RAW_PATH = os.environ.get(
    "ML_TRAIN_RAW_PATH",
)
ML_TEST_RAW_PATH = os.environ.get(
    "ML_TEST_RAW_PATH",
)

ML_TRAIN_ENCODED_PATH = os.environ.get(
    "ML_TRAIN_ENCODED_PATH",
)
ML_TEST_ENCODED_PATH = os.environ.get(
    "ML_TEST_ENCODED_PATH",
)

ML_TRAIN_JSON_PATH = os.environ.get(
    "ML_TRAIN_JSON_PATH",
)
ML_TEST_JSON_PATH = os.environ.get(
    "ML_TEST_JSON_PATH",
)

ML_TRAIN_BALANCED_PATH = os.environ.get(
    "ML_TRAIN_BALANCED_PATH",
)

TRAIN_RATIO = float(os.environ.get("ML_TRAIN_RATIO", "0.7"))
TEST_RATIO = 1.0 - TRAIN_RATIO
SPLIT_SEED = int(os.environ.get("ML_SPLIT_SEED", "42"))

ML_SPLIT_SUMMARY_PATH = os.environ.get("ML_SPLIT_SUMMARY_PATH")
ML_FEATURE_INFO_PATH = os.environ.get("ML_FEATURE_INFO_PATH")
ML_LABEL_DISTRIBUTION_PATH = os.environ.get("ML_LABEL_DISTRIBUTION_PATH")


def build_spark():
    return (
        SparkSession.builder.appName("team25 - build train test datasets")
        .master("yarn")
        .getOrCreate()
    )


def save_csv(df, output_path):
    (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path))


def build_label_distribution_df(df, dataset_name):
    total = df.count()

    distribution_df = (
        df.groupBy("label")
        .count()
        .withColumn("dataset", F.lit(dataset_name))
        .withColumn("share", F.col("count") / F.lit(float(total)))
        .select("dataset", "label", "count", "share")
        .orderBy("label")
    )

    return distribution_df


def build_split_summary_df(
    spark,
    train_raw_count,
    test_raw_count,
    train_encoded_count,
    test_encoded_count,
    feature_count,
):
    rows = [
        ("train_raw", train_raw_count, feature_count),
        ("test_raw", test_raw_count, feature_count),
        ("train_encoded", train_encoded_count, feature_count),
        ("test_encoded", test_encoded_count, feature_count),
    ]

    return spark.createDataFrame(
        rows,
        ["dataset", "rows", "feature_count"],
    )


def build_feature_info_df(spark, numeric_cols, categorical_cols):
    rows = [(name, "numeric") for name in numeric_cols] + [
        (name, "categorical") for name in categorical_cols
    ]

    return spark.createDataFrame(rows, ["feature_name", "feature_type"])


def get_feature_columns():
    numeric_cols = [
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
        "issue_year",
        "issue_month",
        "issue_month_sin",
        "issue_month_cos",
        "credit_history_months",
    ]

    categorical_cols = [
        "grade",
        "sub_grade",
        "emp_length",
        "home_ownership",
        "verification_status",
        "purpose",
        "application_type",
    ]

    return numeric_cols, categorical_cols


def compute_mode_fill_map(df, columns):
    fill_map = {}

    for c in columns:
        mode_row = (
            df.where(F.col(c).isNotNull())
            .groupBy(c)
            .count()
            .orderBy(F.desc("count"), F.asc(c))
            .first()
        )

        if mode_row is not None:
            fill_map[c] = mode_row[0]

    return fill_map


def apply_fill_map(df, fill_map):
    if fill_map:
        return df.fillna(fill_map)
    return df


def build_encoding_pipeline(categorical_cols, numeric_cols):
    stages = []
    indexed_cols = []
    encoded_cols = []

    for c in categorical_cols:
        indexed = "{}_idx".format(c)
        indexed_cols.append(indexed)

        stages.append(
            StringIndexer(inputCol=c, outputCol=indexed, handleInvalid="keep")
        )

    for indexed in indexed_cols:
        encoded = "{}_ohe".format(indexed.replace("_idx", ""))
        encoded_cols.append(encoded)
        stages.append(OneHotEncoder(inputCol=indexed, outputCol=encoded))

    assembler = VectorAssembler(
        inputCols=encoded_cols + numeric_cols, outputCol="features"
    )
    stages.append(assembler)

    return Pipeline(stages=stages)


def balance_train_dataset(train_df):
    """
    Simple upsampling of minority class to match majority class.
    Test set must never be touched.
    """
    class_counts = {
        row["label"]: row["count"]
        for row in train_df.groupBy("label").count().collect()
    }

    count_0 = float(class_counts.get(0.0, 0))
    count_1 = float(class_counts.get(1.0, 0))

    if count_0 == 0 or count_1 == 0:
        return train_df

    if count_0 > count_1:
        majority_label = 0.0
        minority_label = 1.0
        majority_count = count_0
        minority_count = count_1
    else:
        majority_label = 1.0
        minority_label = 0.0
        majority_count = count_1
        minority_count = count_0

    majority_df = train_df.filter(F.col("label") == F.lit(majority_label))
    minority_df = train_df.filter(F.col("label") == F.lit(minority_label))

    ratio = majority_count / minority_count
    whole = int(ratio)
    frac = ratio - whole

    pieces = [minority_df] * max(whole, 1)
    if frac > 0:
        pieces.append(
            minority_df.sample(withReplacement=True, fraction=frac, seed=SPLIT_SEED)
        )

    upsampled_minority = pieces[0]
    for piece in pieces[1:]:
        upsampled_minority = upsampled_minority.union(piece)

    balanced_df = majority_df.union(upsampled_minority)

    return balanced_df.orderBy(F.rand(seed=SPLIT_SEED))


def save_json_artifact(df, output_path):
    """
    Keep only label and features and save as one partition.
    """
    (
        df.select("features", "label")
        .coalesce(1)
        .write.mode("overwrite")
        .json(output_path)
    )

def main():
    spark = build_spark()

    print("Reading prepared raw dataset from:", ML_PREPARED_RAW_PATH)
    df = spark.read.parquet(ML_PREPARED_RAW_PATH)

    print("\nInput schema")
    df.printSchema()

    print("\nInput label distribution")
    df.groupBy("label").count().orderBy("label").show()

    full_label_distribution_df = build_label_distribution_df(df, "full_prepared_raw")

    train_raw, test_raw = df.randomSplit([TRAIN_RATIO, TEST_RATIO], seed=SPLIT_SEED)

    train_raw_count = train_raw.count()
    test_raw_count = test_raw.count()

    print("\nTrain raw rows:", train_raw_count)
    print("Test raw rows:", test_raw_count)

    print("\nTrain raw label distribution")
    train_raw.groupBy("label").count().orderBy("label").show()

    print("\nTest raw label distribution")
    test_raw.groupBy("label").count().orderBy("label").show()

    train_raw_label_distribution_df = build_label_distribution_df(
        train_raw, "train_raw"
    )
    test_raw_label_distribution_df = build_label_distribution_df(
        test_raw, "test_raw"
    )

    print("\nSaving raw train split to:", ML_TRAIN_RAW_PATH)
    train_raw.write.mode("overwrite").parquet(ML_TRAIN_RAW_PATH)

    print("Saving raw test split to:", ML_TEST_RAW_PATH)
    test_raw.write.mode("overwrite").parquet(ML_TEST_RAW_PATH)

    numeric_cols, categorical_cols = get_feature_columns()
    cols_to_fill = numeric_cols + categorical_cols

    print("\nNumeric feature columns")
    print(numeric_cols)

    print("\nCategorical feature columns")
    print(categorical_cols)

    print("\nNumber of raw feature columns:", len(cols_to_fill))

    train_fill_map = compute_mode_fill_map(train_raw, cols_to_fill)
    train_raw_filled = apply_fill_map(train_raw, train_fill_map)
    test_raw_filled = apply_fill_map(test_raw, train_fill_map)

    pipeline = build_encoding_pipeline(
        categorical_cols=categorical_cols,
        numeric_cols=numeric_cols,
    )

    print("\nFitting preprocessing pipeline on the training split")
    pipeline_model = pipeline.fit(train_raw_filled)

    train_encoded_before_balancing = pipeline_model.transform(train_raw_filled).select(
        "id", "loan_status", "label", "features"
    )
    test_encoded = pipeline_model.transform(test_raw_filled).select(
        "id", "loan_status", "label", "features"
    )

    train_encoded_before_balancing_count = train_encoded_before_balancing.count()
    test_encoded_count = test_encoded.count()
    feature_count = train_encoded_before_balancing.select("features").first()["features"].size

    print("\nTrain encoded rows before balancing:", train_encoded_before_balancing_count)
    print("Test encoded rows:", test_encoded_count)
    print("Number of assembled features:", feature_count)

    print("\nTrain encoded label distribution before balancing")
    train_encoded_before_balancing.groupBy("label").count().orderBy("label").show()

    print("\nTest encoded label distribution")
    test_encoded.groupBy("label").count().orderBy("label").show()

    train_encoded_before_balancing_label_distribution_df = build_label_distribution_df(
        train_encoded_before_balancing,
        "train_encoded_before_balancing",
    )
    test_encoded_label_distribution_df = build_label_distribution_df(
        test_encoded,
        "test_encoded",
    )

    print("\nBalancing encoded train dataset")
    train_encoded = balance_train_dataset(train_encoded_before_balancing)
    train_encoded_count = train_encoded.count()

    print("\nTrain encoded rows after balancing:", train_encoded_count)

    print("\nTrain encoded label distribution after balancing")
    train_encoded.groupBy("label").count().orderBy("label").show()

    train_encoded_label_distribution_df = build_label_distribution_df(
        train_encoded,
        "train_encoded",
    )

    print("\nTrain encoded sample")
    train_encoded.show(10, truncate=False)

    print("\nTest encoded sample")
    test_encoded.show(10, truncate=False)

    print("\nSaving balanced encoded train dataset to:", ML_TRAIN_ENCODED_PATH)
    train_encoded.write.mode("overwrite").parquet(ML_TRAIN_ENCODED_PATH)

    print("Saving encoded test dataset to:", ML_TEST_ENCODED_PATH)
    test_encoded.write.mode("overwrite").parquet(ML_TEST_ENCODED_PATH)

    print("Saving train JSON artifact to:", ML_TRAIN_JSON_PATH)
    save_json_artifact(train_encoded, ML_TRAIN_JSON_PATH)

    print("Saving test JSON artifact to:", ML_TEST_JSON_PATH)
    save_json_artifact(test_encoded, ML_TEST_JSON_PATH)

    split_summary_df = build_split_summary_df(
        spark,
        train_raw_count=train_raw_count,
        test_raw_count=test_raw_count,
        train_encoded_count=train_encoded_count,
        test_encoded_count=test_encoded_count,
        feature_count=feature_count,
    )

    feature_info_df = build_feature_info_df(
        spark,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
    )

    label_distribution_df = (
        full_label_distribution_df
        .unionByName(train_raw_label_distribution_df)
        .unionByName(test_raw_label_distribution_df)
        .unionByName(train_encoded_before_balancing_label_distribution_df)
        .unionByName(train_encoded_label_distribution_df)
        .unionByName(test_encoded_label_distribution_df)
    )

    print("\nSaving split summary CSV to:", ML_SPLIT_SUMMARY_PATH)
    save_csv(split_summary_df, ML_SPLIT_SUMMARY_PATH)

    print("Saving feature info CSV to:", ML_FEATURE_INFO_PATH)
    save_csv(feature_info_df, ML_FEATURE_INFO_PATH)

    print("Saving label distribution CSV to:", ML_LABEL_DISTRIBUTION_PATH)
    save_csv(label_distribution_df, ML_LABEL_DISTRIBUTION_PATH)

    print("\nTrain and test datasets are ready")
    spark.stop()


if __name__ == "__main__":
    main()
