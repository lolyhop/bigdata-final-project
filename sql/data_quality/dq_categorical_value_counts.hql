DROP TABLE IF EXISTS dq_categorical_value_counts_results;

CREATE TABLE dq_categorical_value_counts_results
STORED AS ORC
AS
WITH counts AS (
    SELECT
        'grade' AS feature,
        COALESCE(grade, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(grade, 'NULL')

    UNION ALL

    SELECT
        'sub_grade' AS feature,
        COALESCE(sub_grade, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(sub_grade, 'NULL')

    UNION ALL

    SELECT
        'emp_length' AS feature,
        COALESCE(emp_length, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(emp_length, 'NULL')

    UNION ALL

    SELECT
        'home_ownership' AS feature,
        COALESCE(home_ownership, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(home_ownership, 'NULL')

    UNION ALL

    SELECT
        'verification_status' AS feature,
        COALESCE(verification_status, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(verification_status, 'NULL')

    UNION ALL

    SELECT
        'purpose' AS feature,
        COALESCE(purpose, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(purpose, 'NULL')

    UNION ALL

    SELECT
        'application_type' AS feature,
        COALESCE(application_type, 'NULL') AS value,
        COUNT(*) AS count
    FROM loans
    GROUP BY COALESCE(application_type, 'NULL')
)
SELECT
    feature,
    value,
    count,
    ROUND(
        count / SUM(count) OVER (PARTITION BY feature) * 100,
        4
    ) AS share_pct
FROM counts;