DROP TABLE IF EXISTS q2_results;

CREATE TABLE q2_results (
    purpose STRING,
    total_loans BIGINT,
    defaulted BIGINT,
    default_rate DOUBLE,
    avg_loan_amnt DOUBLE
)
STORED AS ORC;

INSERT INTO q2_results
SELECT
    purpose,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) AS defaulted,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) / COUNT(*) * 100,
        2
    ) AS default_rate,
    ROUND(AVG(loan_amnt), 2) AS avg_loan_amnt
FROM loans
WHERE loan_status IN (
    'Fully Paid', 'Charged Off', 'Default',
    'Does not meet the credit policy. Status:Fully Paid',
    'Does not meet the credit policy. Status:Charged Off'
)
GROUP BY purpose
ORDER BY default_rate DESC;

ANALYZE TABLE q2_results COMPUTE STATISTICS;
