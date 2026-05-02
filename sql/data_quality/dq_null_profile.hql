DROP TABLE IF EXISTS dq_null_profile_results;

CREATE TABLE dq_null_profile_results
STORED AS ORC
AS
WITH summary AS (
    SELECT
        COUNT(*) AS total_rows,

        SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_null_count,
        SUM(CASE WHEN loan_status IS NULL THEN 1 ELSE 0 END) AS loan_status_null_count,
        SUM(CASE WHEN loan_amnt IS NULL THEN 1 ELSE 0 END) AS loan_amnt_null_count,
        SUM(CASE WHEN term IS NULL THEN 1 ELSE 0 END) AS term_null_count,
        SUM(CASE WHEN int_rate IS NULL THEN 1 ELSE 0 END) AS int_rate_null_count,
        SUM(CASE WHEN installment IS NULL THEN 1 ELSE 0 END) AS installment_null_count,
        SUM(CASE WHEN grade IS NULL THEN 1 ELSE 0 END) AS grade_null_count,
        SUM(CASE WHEN sub_grade IS NULL THEN 1 ELSE 0 END) AS sub_grade_null_count,
        SUM(CASE WHEN emp_length IS NULL THEN 1 ELSE 0 END) AS emp_length_null_count,
        SUM(CASE WHEN home_ownership IS NULL THEN 1 ELSE 0 END) AS home_ownership_null_count,
        SUM(CASE WHEN annual_inc IS NULL THEN 1 ELSE 0 END) AS annual_inc_null_count,
        SUM(CASE WHEN verification_status IS NULL THEN 1 ELSE 0 END) AS verification_status_null_count,
        SUM(CASE WHEN purpose IS NULL THEN 1 ELSE 0 END) AS purpose_null_count,
        SUM(CASE WHEN dti IS NULL THEN 1 ELSE 0 END) AS dti_null_count,
        SUM(CASE WHEN delinq_2yrs IS NULL THEN 1 ELSE 0 END) AS delinq_2yrs_null_count,
        SUM(CASE WHEN earliest_cr_line IS NULL THEN 1 ELSE 0 END) AS earliest_cr_line_null_count,
        SUM(CASE WHEN inq_last_6mths IS NULL THEN 1 ELSE 0 END) AS inq_last_6mths_null_count,
        SUM(CASE WHEN open_acc IS NULL THEN 1 ELSE 0 END) AS open_acc_null_count,
        SUM(CASE WHEN pub_rec IS NULL THEN 1 ELSE 0 END) AS pub_rec_null_count,
        SUM(CASE WHEN revol_bal IS NULL THEN 1 ELSE 0 END) AS revol_bal_null_count,
        SUM(CASE WHEN revol_util IS NULL THEN 1 ELSE 0 END) AS revol_util_null_count,
        SUM(CASE WHEN total_acc IS NULL THEN 1 ELSE 0 END) AS total_acc_null_count,
        SUM(CASE WHEN application_type IS NULL THEN 1 ELSE 0 END) AS application_type_null_count,
        SUM(CASE WHEN fico_range_low IS NULL THEN 1 ELSE 0 END) AS fico_range_low_null_count,
        SUM(CASE WHEN fico_range_high IS NULL THEN 1 ELSE 0 END) AS fico_range_high_null_count,
        SUM(CASE WHEN issue_d IS NULL THEN 1 ELSE 0 END) AS issue_d_null_count
    FROM loans
)
SELECT
    column_name,
    CAST(null_count AS BIGINT) AS null_count,
    CAST(total_rows AS BIGINT) AS total_rows,
    ROUND(CAST(null_count AS DOUBLE) / CAST(total_rows AS DOUBLE) * 100, 4) AS null_pct
FROM summary
LATERAL VIEW STACK(
    26,

    'id', id_null_count,
    'loan_status', loan_status_null_count,
    'loan_amnt', loan_amnt_null_count,
    'term', term_null_count,
    'int_rate', int_rate_null_count,
    'installment', installment_null_count,
    'grade', grade_null_count,
    'sub_grade', sub_grade_null_count,
    'emp_length', emp_length_null_count,
    'home_ownership', home_ownership_null_count,
    'annual_inc', annual_inc_null_count,
    'verification_status', verification_status_null_count,
    'purpose', purpose_null_count,
    'dti', dti_null_count,
    'delinq_2yrs', delinq_2yrs_null_count,
    'earliest_cr_line', earliest_cr_line_null_count,
    'inq_last_6mths', inq_last_6mths_null_count,
    'open_acc', open_acc_null_count,
    'pub_rec', pub_rec_null_count,
    'revol_bal', revol_bal_null_count,
    'revol_util', revol_util_null_count,
    'total_acc', total_acc_null_count,
    'application_type', application_type_null_count,
    'fico_range_low', fico_range_low_null_count,
    'fico_range_high', fico_range_high_null_count,
    'issue_d', issue_d_null_count
) stacked AS column_name, null_count;