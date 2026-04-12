DROP TABLE IF EXISTS {table_name};

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
    loan_status TEXT,

    CONSTRAINT chk_loan_nonnegative_counts CHECK (
        (delinq_2yrs IS NULL OR delinq_2yrs >= 0) AND
        (inq_last_6mths IS NULL OR inq_last_6mths >= 0) AND
        (open_acc IS NULL OR open_acc >= 0) AND
        (pub_rec IS NULL OR pub_rec >= 0) AND
        (total_acc IS NULL OR total_acc >= 0)
    ),
    CONSTRAINT chk_loan_fico_order CHECK (
        fico_range_low IS NULL OR fico_range_high IS NULL OR fico_range_low <= fico_range_high
    ),
    CONSTRAINT chk_loan_term_positive CHECK (term IS NULL OR term > 0),
    CONSTRAINT chk_loan_revol_bal_nonnegative CHECK (revol_bal IS NULL OR revol_bal >= 0),
    CONSTRAINT chk_loan_numeric_domains CHECK (
        (loan_amnt IS NULL OR loan_amnt > 0) AND
        (installment IS NULL OR installment >= 0) AND
        (annual_inc IS NULL OR annual_inc >= 0) AND
        (int_rate IS NULL OR (int_rate >= 0 AND int_rate <= 100)) AND
        (revol_util IS NULL OR revol_util >= 0) AND
        (dti IS NULL OR (dti >= -1 AND dti < 1e7))
    )
);