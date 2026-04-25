DROP TABLE IF EXISTS {hive_database}.ml_split_summary;
CREATE EXTERNAL TABLE {hive_database}.ml_split_summary (
    dataset       STRING,
    `rows`        BIGINT,
    feature_count INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{split_summary_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS {hive_database}.ml_feature_info;
CREATE EXTERNAL TABLE {hive_database}.ml_feature_info (
    feature_name STRING,
    feature_type STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{feature_info_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS {hive_database}.ml_label_distribution;
CREATE EXTERNAL TABLE {hive_database}.ml_label_distribution (
    dataset  STRING,
    `label`  DOUBLE,
    `count`  BIGINT,
    share    DOUBLE
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{label_distribution_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS {hive_database}.ml_baseline_evaluation;
CREATE EXTERNAL TABLE {hive_database}.ml_baseline_evaluation (
    model        STRING,
    accuracy     DOUBLE,
    `precision`  DOUBLE,
    recall       DOUBLE,
    f1           DOUBLE,
    areaUnderROC DOUBLE,
    areaUnderPR  DOUBLE,
    tp           BIGINT,
    tn           BIGINT,
    fp           BIGINT,
    fn           BIGINT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{baseline_evaluation_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS {hive_database}.ml_evaluation;
CREATE EXTERNAL TABLE {hive_database}.ml_evaluation (
    model        STRING,
    accuracy     DOUBLE,
    `precision`  DOUBLE,
    recall       DOUBLE,
    f1           DOUBLE,
    areaUnderROC DOUBLE,
    areaUnderPR  DOUBLE,
    tp           BIGINT,
    tn           BIGINT,
    fp           BIGINT,
    fn           BIGINT,
    best_params  STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{evaluation_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS {hive_database}.ml_prediction_distribution;
CREATE EXTERNAL TABLE {hive_database}.ml_prediction_distribution (
    model        STRING,
    `label`      DOUBLE,
    `prediction` DOUBLE,
    `count`      BIGINT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{prediction_distribution_path}'
TBLPROPERTIES ('skip.header.line.count'='1');
