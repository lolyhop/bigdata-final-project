from __future__ import annotations

from typing import Final

LOAN_CSV_COLUMNS: Final[tuple[str, ...]] = (
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
    loan_amnt NUMERIC,
    term INTEGER,
    int_rate NUMERIC,
    installment NUMERIC,
    grade TEXT,
    sub_grade TEXT,
    emp_length TEXT,
    home_ownership TEXT,
    annual_inc NUMERIC,
    verification_status TEXT,
    purpose TEXT,
    dti NUMERIC,
    delinq_2yrs INTEGER,
    inq_last_6mths INTEGER,
    open_acc INTEGER,
    pub_rec INTEGER,
    revol_bal BIGINT,
    revol_util NUMERIC,
    total_acc INTEGER,
    application_type TEXT,
    fico_range_low INTEGER,
    fico_range_high INTEGER,
    issue_d DATE,
    earliest_cr_line DATE,
    loan_status TEXT
);
"""
