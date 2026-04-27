DROP TABLE IF EXISTS dq_numeric_summary_results;

CREATE TABLE dq_numeric_summary_results
STORED AS ORC
AS
SELECT
    'loan_amnt' AS feature,
    COUNT(loan_amnt) AS non_null_count,
    SUM(CASE WHEN loan_amnt IS NULL THEN 1 ELSE 0 END) AS null_count,
    MIN(CAST(loan_amnt AS DOUBLE)) AS min_value,
    percentile_approx(CAST(loan_amnt AS DOUBLE), 0.25) AS p25,
    percentile_approx(CAST(loan_amnt AS DOUBLE), 0.50) AS median,
    percentile_approx(CAST(loan_amnt AS DOUBLE), 0.75) AS p75,
    percentile_approx(CAST(loan_amnt AS DOUBLE), 0.95) AS p95,
    MAX(CAST(loan_amnt AS DOUBLE)) AS max_value,
    AVG(CAST(loan_amnt AS DOUBLE)) AS avg_value,
    STDDEV(CAST(loan_amnt AS DOUBLE)) AS stddev_value
FROM team25_projectdb.loans

UNION ALL

SELECT 'term', COUNT(term), SUM(CASE WHEN term IS NULL THEN 1 ELSE 0 END), MIN(CAST(term AS DOUBLE)), percentile_approx(CAST(term AS DOUBLE), 0.25), percentile_approx(CAST(term AS DOUBLE), 0.50), percentile_approx(CAST(term AS DOUBLE), 0.75), percentile_approx(CAST(term AS DOUBLE), 0.95), MAX(CAST(term AS DOUBLE)), AVG(CAST(term AS DOUBLE)), STDDEV(CAST(term AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'int_rate', COUNT(int_rate), SUM(CASE WHEN int_rate IS NULL THEN 1 ELSE 0 END), MIN(CAST(int_rate AS DOUBLE)), percentile_approx(CAST(int_rate AS DOUBLE), 0.25), percentile_approx(CAST(int_rate AS DOUBLE), 0.50), percentile_approx(CAST(int_rate AS DOUBLE), 0.75), percentile_approx(CAST(int_rate AS DOUBLE), 0.95), MAX(CAST(int_rate AS DOUBLE)), AVG(CAST(int_rate AS DOUBLE)), STDDEV(CAST(int_rate AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'installment', COUNT(installment), SUM(CASE WHEN installment IS NULL THEN 1 ELSE 0 END), MIN(CAST(installment AS DOUBLE)), percentile_approx(CAST(installment AS DOUBLE), 0.25), percentile_approx(CAST(installment AS DOUBLE), 0.50), percentile_approx(CAST(installment AS DOUBLE), 0.75), percentile_approx(CAST(installment AS DOUBLE), 0.95), MAX(CAST(installment AS DOUBLE)), AVG(CAST(installment AS DOUBLE)), STDDEV(CAST(installment AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'annual_inc', COUNT(annual_inc), SUM(CASE WHEN annual_inc IS NULL THEN 1 ELSE 0 END), MIN(CAST(annual_inc AS DOUBLE)), percentile_approx(CAST(annual_inc AS DOUBLE), 0.25), percentile_approx(CAST(annual_inc AS DOUBLE), 0.50), percentile_approx(CAST(annual_inc AS DOUBLE), 0.75), percentile_approx(CAST(annual_inc AS DOUBLE), 0.95), MAX(CAST(annual_inc AS DOUBLE)), AVG(CAST(annual_inc AS DOUBLE)), STDDEV(CAST(annual_inc AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'dti', COUNT(dti), SUM(CASE WHEN dti IS NULL THEN 1 ELSE 0 END), MIN(CAST(dti AS DOUBLE)), percentile_approx(CAST(dti AS DOUBLE), 0.25), percentile_approx(CAST(dti AS DOUBLE), 0.50), percentile_approx(CAST(dti AS DOUBLE), 0.75), percentile_approx(CAST(dti AS DOUBLE), 0.95), MAX(CAST(dti AS DOUBLE)), AVG(CAST(dti AS DOUBLE)), STDDEV(CAST(dti AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'delinq_2yrs', COUNT(delinq_2yrs), SUM(CASE WHEN delinq_2yrs IS NULL THEN 1 ELSE 0 END), MIN(CAST(delinq_2yrs AS DOUBLE)), percentile_approx(CAST(delinq_2yrs AS DOUBLE), 0.25), percentile_approx(CAST(delinq_2yrs AS DOUBLE), 0.50), percentile_approx(CAST(delinq_2yrs AS DOUBLE), 0.75), percentile_approx(CAST(delinq_2yrs AS DOUBLE), 0.95), MAX(CAST(delinq_2yrs AS DOUBLE)), AVG(CAST(delinq_2yrs AS DOUBLE)), STDDEV(CAST(delinq_2yrs AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'inq_last_6mths', COUNT(inq_last_6mths), SUM(CASE WHEN inq_last_6mths IS NULL THEN 1 ELSE 0 END), MIN(CAST(inq_last_6mths AS DOUBLE)), percentile_approx(CAST(inq_last_6mths AS DOUBLE), 0.25), percentile_approx(CAST(inq_last_6mths AS DOUBLE), 0.50), percentile_approx(CAST(inq_last_6mths AS DOUBLE), 0.75), percentile_approx(CAST(inq_last_6mths AS DOUBLE), 0.95), MAX(CAST(inq_last_6mths AS DOUBLE)), AVG(CAST(inq_last_6mths AS DOUBLE)), STDDEV(CAST(inq_last_6mths AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'open_acc', COUNT(open_acc), SUM(CASE WHEN open_acc IS NULL THEN 1 ELSE 0 END), MIN(CAST(open_acc AS DOUBLE)), percentile_approx(CAST(open_acc AS DOUBLE), 0.25), percentile_approx(CAST(open_acc AS DOUBLE), 0.50), percentile_approx(CAST(open_acc AS DOUBLE), 0.75), percentile_approx(CAST(open_acc AS DOUBLE), 0.95), MAX(CAST(open_acc AS DOUBLE)), AVG(CAST(open_acc AS DOUBLE)), STDDEV(CAST(open_acc AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'pub_rec', COUNT(pub_rec), SUM(CASE WHEN pub_rec IS NULL THEN 1 ELSE 0 END), MIN(CAST(pub_rec AS DOUBLE)), percentile_approx(CAST(pub_rec AS DOUBLE), 0.25), percentile_approx(CAST(pub_rec AS DOUBLE), 0.50), percentile_approx(CAST(pub_rec AS DOUBLE), 0.75), percentile_approx(CAST(pub_rec AS DOUBLE), 0.95), MAX(CAST(pub_rec AS DOUBLE)), AVG(CAST(pub_rec AS DOUBLE)), STDDEV(CAST(pub_rec AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'revol_bal', COUNT(revol_bal), SUM(CASE WHEN revol_bal IS NULL THEN 1 ELSE 0 END), MIN(CAST(revol_bal AS DOUBLE)), percentile_approx(CAST(revol_bal AS DOUBLE), 0.25), percentile_approx(CAST(revol_bal AS DOUBLE), 0.50), percentile_approx(CAST(revol_bal AS DOUBLE), 0.75), percentile_approx(CAST(revol_bal AS DOUBLE), 0.95), MAX(CAST(revol_bal AS DOUBLE)), AVG(CAST(revol_bal AS DOUBLE)), STDDEV(CAST(revol_bal AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'revol_util', COUNT(revol_util), SUM(CASE WHEN revol_util IS NULL THEN 1 ELSE 0 END), MIN(CAST(revol_util AS DOUBLE)), percentile_approx(CAST(revol_util AS DOUBLE), 0.25), percentile_approx(CAST(revol_util AS DOUBLE), 0.50), percentile_approx(CAST(revol_util AS DOUBLE), 0.75), percentile_approx(CAST(revol_util AS DOUBLE), 0.95), MAX(CAST(revol_util AS DOUBLE)), AVG(CAST(revol_util AS DOUBLE)), STDDEV(CAST(revol_util AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'total_acc', COUNT(total_acc), SUM(CASE WHEN total_acc IS NULL THEN 1 ELSE 0 END), MIN(CAST(total_acc AS DOUBLE)), percentile_approx(CAST(total_acc AS DOUBLE), 0.25), percentile_approx(CAST(total_acc AS DOUBLE), 0.50), percentile_approx(CAST(total_acc AS DOUBLE), 0.75), percentile_approx(CAST(total_acc AS DOUBLE), 0.95), MAX(CAST(total_acc AS DOUBLE)), AVG(CAST(total_acc AS DOUBLE)), STDDEV(CAST(total_acc AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'fico_range_low', COUNT(fico_range_low), SUM(CASE WHEN fico_range_low IS NULL THEN 1 ELSE 0 END), MIN(CAST(fico_range_low AS DOUBLE)), percentile_approx(CAST(fico_range_low AS DOUBLE), 0.25), percentile_approx(CAST(fico_range_low AS DOUBLE), 0.50), percentile_approx(CAST(fico_range_low AS DOUBLE), 0.75), percentile_approx(CAST(fico_range_low AS DOUBLE), 0.95), MAX(CAST(fico_range_low AS DOUBLE)), AVG(CAST(fico_range_low AS DOUBLE)), STDDEV(CAST(fico_range_low AS DOUBLE))
FROM team25_projectdb.loans

UNION ALL

SELECT 'fico_range_high', COUNT(fico_range_high), SUM(CASE WHEN fico_range_high IS NULL THEN 1 ELSE 0 END), MIN(CAST(fico_range_high AS DOUBLE)), percentile_approx(CAST(fico_range_high AS DOUBLE), 0.25), percentile_approx(CAST(fico_range_high AS DOUBLE), 0.50), percentile_approx(CAST(fico_range_high AS DOUBLE), 0.75), percentile_approx(CAST(fico_range_high AS DOUBLE), 0.95), MAX(CAST(fico_range_high AS DOUBLE)), AVG(CAST(fico_range_high AS DOUBLE)), STDDEV(CAST(fico_range_high AS DOUBLE))
FROM team25_projectdb.loans;