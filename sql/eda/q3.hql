DROP TABLE IF EXISTS q3_results;

CREATE TABLE q3_results (
    issue_year INT,
    total_loans BIGINT,
    defaulted BIGINT,
    default_rate DOUBLE,
    avg_int_rate DOUBLE
)
STORED AS ORC;

INSERT INTO q3_results
SELECT
    YEAR(issue_d) AS issue_year,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) AS defaulted,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) / COUNT(*) * 100,
        2
    ) AS default_rate,
    ROUND(AVG(int_rate), 2) AS avg_int_rate
FROM loans
WHERE loan_status IN (
    'Fully Paid', 'Charged Off', 'Default',
    'Does not meet the credit policy. Status:Fully Paid',
    'Does not meet the credit policy. Status:Charged Off'
)
GROUP BY YEAR(issue_d)
ORDER BY issue_year;

ANALYZE TABLE q3_results COMPUTE STATISTICS;
