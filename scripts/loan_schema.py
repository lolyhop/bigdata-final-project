from typing import Tuple

from typing_extensions import Final

LOAN_CSV_COLUMNS: Final[Tuple[str, ...]] = (
    "id",
    "loan_amnt",
    "term",
    "int_rate",
    "installment",
    "grade",
    "sub_grade",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "purpose",
    "dti",
    "delinq_2yrs",
    "inq_last_6mths",
    "open_acc",
    "pub_rec",
    "revol_bal",
    "revol_util",
    "total_acc",
    "application_type",
    "fico_range_low",
    "fico_range_high",
    "issue_d",
    "earliest_cr_line",
    "loan_status",
)

LOAN_TABLE_DDL_TEMPLATE: Final[str] = """
CREATE TABLE {table_name} (
    id BIGINT PRIMARY KEY,
    loan_amnt DOUBLE PRECISION,
    term INTEGER,
    int_rate DOUBLE PRECISION,
    installment DOUBLE PRECISION,
    grade TEXT,
    sub_grade TEXT,
    emp_length TEXT,
    home_ownership TEXT,
    annual_inc DOUBLE PRECISION,
    verification_status TEXT,
    purpose TEXT,
    dti DOUBLE PRECISION,
    delinq_2yrs INTEGER,
    inq_last_6mths INTEGER,
    open_acc INTEGER,
    pub_rec INTEGER,
    revol_bal BIGINT,
    revol_util DOUBLE PRECISION,
    total_acc INTEGER,
    application_type TEXT,
    fico_range_low INTEGER,
    fico_range_high INTEGER,
    issue_d DATE,
    earliest_cr_line DATE,
    loan_status TEXT
);
"""

HIVE_STAGING_TABLE_DDL_TEMPLATE: Final[str] = """
CREATE EXTERNAL TABLE {staging_table} (
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
LOCATION '{hdfs_data_dir}'
"""

HIVE_FINAL_TABLE_CTAS_TEMPLATE: Final[str] = """
CREATE TABLE {final_table}
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
FROM {staging_table}
"""
