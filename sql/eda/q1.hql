-- Q1: Default rate by loan grade
-- Analyzes completed loans only (excludes Current, Late, In Grace Period).
-- Terminal loan statuses: Fully Paid, Charged Off, Default, and policy variants.
-- Defaulted statuses: Charged Off, Default, policy:Charged Off.

DROP TABLE IF EXISTS q1_results;

CREATE TABLE q1_results (
    grade        STRING,
    total_loans  BIGINT,
    defaulted    BIGINT,
    fully_paid   BIGINT,
    default_rate DOUBLE,
    avg_int_rate DOUBLE
)
STORED AS ORC;

INSERT INTO q1_results
SELECT
    grade,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END) AS defaulted,
    SUM(CASE WHEN loan_status IN ('Fully Paid', 'Does not meet the credit policy. Status:Fully Paid') THEN 1 ELSE 0 END) AS fully_paid,
    ROUND(
        SUM(CASE WHEN loan_status IN ('Charged Off', 'Default', 'Does not meet the credit policy. Status:Charged Off') THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS default_rate,
    ROUND(AVG(int_rate), 2) AS avg_int_rate
FROM loans
WHERE loan_status IN ('Fully Paid', 'Charged Off', 'Default', 'Does not meet the credit policy. Status:Fully Paid', 'Does not meet the credit policy. Status:Charged Off')
GROUP BY grade
ORDER BY grade;

ANALYZE TABLE q1_results COMPUTE STATISTICS;