DROP TABLE IF EXISTS dq_categorical_value_counts_results;

CREATE TABLE dq_categorical_value_counts_results
STORED AS ORC
AS
WITH base AS (
    SELECT *
    FROM team25_projectdb.loans
),
total AS (
    SELECT COUNT(*) AS total_rows
    FROM base
)
SELECT 'grade' AS feature, COALESCE(grade, 'NULL') AS value, COUNT(*) AS count, ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4) AS share_pct
FROM base CROSS JOIN total
GROUP BY COALESCE(grade, 'NULL')

UNION ALL

SELECT 'sub_grade', COALESCE(sub_grade, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(sub_grade, 'NULL')

UNION ALL

SELECT 'emp_length', COALESCE(emp_length, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(emp_length, 'NULL')

UNION ALL

SELECT 'home_ownership', COALESCE(home_ownership, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(home_ownership, 'NULL')

UNION ALL

SELECT 'verification_status', COALESCE(verification_status, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(verification_status, 'NULL')

UNION ALL

SELECT 'purpose', COALESCE(purpose, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(purpose, 'NULL')

UNION ALL

SELECT 'application_type', COALESCE(application_type, 'NULL'), COUNT(*), ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY COALESCE(application_type, 'NULL');