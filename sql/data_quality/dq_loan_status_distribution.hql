DROP TABLE IF EXISTS dq_loan_status_distribution_results;

CREATE TABLE dq_loan_status_distribution_results
STORED AS ORC
AS
WITH total AS (
    SELECT COUNT(*) AS total_rows
    FROM loans
)
SELECT
    COALESCE(loan_status, 'NULL') AS loan_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4) AS share_pct
FROM loans
CROSS JOIN total
GROUP BY COALESCE(loan_status, 'NULL')
ORDER BY count DESC;