DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results (
    grade STRING,
    home_ownership STRING,
    total_loans BIGINT,
    default_rate DOUBLE
)
STORED AS ORC;

INSERT INTO q5_results
SELECT
    grade,
    home_ownership,
    COUNT(*) AS total_loans,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) / COUNT(*) * 100,
        2
    ) AS default_rate
FROM loans
WHERE loan_status IN (
    'Fully Paid', 'Charged Off', 'Default',
    'Does not meet the credit policy. Status:Fully Paid',
    'Does not meet the credit policy. Status:Charged Off'
)
AND home_ownership IN ('RENT', 'OWN', 'MORTGAGE')
GROUP BY grade, home_ownership
ORDER BY grade, home_ownership;

ANALYZE TABLE q5_results COMPUTE STATISTICS;
