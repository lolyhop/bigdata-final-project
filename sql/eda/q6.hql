DROP TABLE IF EXISTS q6_results;

CREATE TABLE q6_results (
    fico_bucket STRING,
    avg_fico DOUBLE,
    total_loans BIGINT,
    default_rate DOUBLE
)
STORED AS ORC;

INSERT INTO q6_results
SELECT
    CASE
        WHEN fico_range_low < 650 THEN '610-650'
        WHEN fico_range_low < 675 THEN '650-675'
        WHEN fico_range_low < 700 THEN '675-700'
        WHEN fico_range_low < 725 THEN '700-725'
        WHEN fico_range_low < 750 THEN '725-750'
        ELSE '750+'
    END AS fico_bucket,
    ROUND(AVG((fico_range_low + fico_range_high) / 2.0), 1) AS avg_fico,
    COUNT(*) AS total_loans,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS default_rate
FROM loans
WHERE loan_status IN (
    'Fully Paid', 'Charged Off', 'Default',
    'Does not meet the credit policy. Status:Fully Paid',
    'Does not meet the credit policy. Status:Charged Off'
)
GROUP BY
    CASE
        WHEN fico_range_low < 650 THEN '610-650'
        WHEN fico_range_low < 675 THEN '650-675'
        WHEN fico_range_low < 700 THEN '675-700'
        WHEN fico_range_low < 725 THEN '700-725'
        WHEN fico_range_low < 750 THEN '725-750'
        ELSE '750+'
    END
ORDER BY avg_fico;

ANALYZE TABLE q6_results COMPUTE STATISTICS;
