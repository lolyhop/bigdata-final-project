DROP TABLE IF EXISTS dq_binary_target_distribution_results;

CREATE TABLE dq_binary_target_distribution_results
STORED AS ORC
AS
WITH binary_loans AS (
    SELECT
        loan_status,
        CASE
            WHEN loan_status = 'Charged Off' THEN 1
            WHEN loan_status = 'Fully Paid' THEN 0
        END AS label
    FROM team25_projectdb.loans
    WHERE loan_status IN ('Fully Paid', 'Charged Off')
),
total AS (
    SELECT COUNT(*) AS total_rows
    FROM binary_loans
)
SELECT
    label,
    loan_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4) AS share_pct
FROM binary_loans
CROSS JOIN total
GROUP BY label, loan_status
ORDER BY label;