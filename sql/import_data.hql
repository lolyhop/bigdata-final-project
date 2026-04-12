DROP TABLE IF EXISTS {final_table};
DROP TABLE IF EXISTS {staging_table_1};
DROP TABLE IF EXISTS {staging_table_2};

CREATE EXTERNAL TABLE {staging_table_1} (
    id BIGINT,
    loan_amnt DOUBLE,
    term INT,
    int_rate DOUBLE,
    installment DOUBLE,
    grade STRING,
    sub_grade STRING,
    emp_length STRING,
    home_ownership STRING,
    annual_inc DOUBLE,
    verification_status STRING,
    purpose STRING,
    dti DOUBLE,
    delinq_2yrs INT,
    inq_last_6mths INT,
    open_acc INT,
    pub_rec INT,
    revol_bal BIGINT,
    revol_util DOUBLE,
    total_acc INT,
    application_type STRING,
    fico_range_low INT,
    fico_range_high INT,
    issue_d BIGINT,
    earliest_cr_line BIGINT,
    loan_status STRING
)
STORED AS PARQUET
LOCATION '{hdfs_data_dir}';

CREATE TABLE {staging_table_2}
STORED AS PARQUET
AS
SELECT
    id,
    loan_amnt,
    term,
    int_rate,
    installment,
    grade,
    sub_grade,
    emp_length,
    home_ownership,
    annual_inc,
    verification_status,
    purpose,
    dti,
    delinq_2yrs,
    inq_last_6mths,
    open_acc,
    pub_rec,
    revol_bal,
    revol_util,
    total_acc,
    application_type,
    fico_range_low,
    fico_range_high,
    CAST(TO_DATE(FROM_UNIXTIME(CAST(CAST(issue_d AS BIGINT) / 1000 AS BIGINT))) AS DATE) AS issue_d,
    CAST(TO_DATE(FROM_UNIXTIME(CAST(CAST(earliest_cr_line AS BIGINT) / 1000 AS BIGINT))) AS DATE) AS earliest_cr_line,
    loan_status
FROM {staging_table_1};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE {final_table} (
    id BIGINT,
    loan_amnt DOUBLE,
    term INT,
    int_rate DOUBLE,
    installment DOUBLE,
    sub_grade STRING,
    emp_length STRING,
    home_ownership STRING,
    annual_inc DOUBLE,
    verification_status STRING,
    purpose STRING,
    dti DOUBLE,
    delinq_2yrs INT,
    inq_last_6mths INT,
    open_acc INT,
    pub_rec INT,
    revol_bal BIGINT,
    revol_util DOUBLE,
    total_acc INT,
    application_type STRING,
    fico_range_low INT,
    fico_range_high INT,
    issue_d DATE,
    earliest_cr_line DATE,
    loan_status STRING
)
PARTITIONED BY (grade STRING)
CLUSTERED BY (id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '{hive_warehouse_dir}/{final_table}'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO {final_table} PARTITION (grade)
SELECT
    id,
    loan_amnt,
    term,
    int_rate,
    installment,
    sub_grade,
    emp_length,
    home_ownership,
    annual_inc,
    verification_status,
    purpose,
    dti,
    delinq_2yrs,
    inq_last_6mths,
    open_acc,
    pub_rec,
    revol_bal,
    revol_util,
    total_acc,
    application_type,
    fico_range_low,
    fico_range_high,
    issue_d,
    earliest_cr_line,
    loan_status,
    grade
FROM {staging_table_2};

DROP TABLE IF EXISTS {staging_table_1};
DROP TABLE IF EXISTS {staging_table_2};
