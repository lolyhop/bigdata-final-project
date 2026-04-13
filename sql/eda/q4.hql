DROP TABLE IF EXISTS q4_results;

CREATE TABLE q4_results (
    dti_bucket STRING,
    total_loans BIGINT,
    defaulted BIGINT,
    default_rate DOUBLE,
    avg_int_rate DOUBLE
)
STORED AS ORC;

INSERT INTO q4_results
SELECT
    CASE
        WHEN dti < 10 THEN '00-10'
        WHEN dti < 20 THEN '10-20'
        WHEN dti < 30 THEN '20-30'
        WHEN dti < 40 THEN '30-40'
        ELSE '40+'
    END AS dti_bucket,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) AS defaulted,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS default_rate,
    ROUND(AVG(int_rate), 2) AS avg_int_rate
FROM loans
WHERE loan_status IN (
    'Fully Paid', 'Charged Off', 'Default',
    'Does not meet the credit policy. Status:Fully Paid',
    'Does not meet the credit policy. Status:Charged Off'
)
  AND dti >= 0 AND dti <= 100
GROUP BY
    CASE
        WHEN dti < 10 THEN '00-10'
        WHEN dti < 20 THEN '10-20'
        WHEN dti < 30 THEN '20-30'
        WHEN dti < 40 THEN '30-40'
        ELSE '40+'
    END
ORDER BY dti_bucket;

ANALYZE TABLE q4_results COMPUTE STATISTICS;
