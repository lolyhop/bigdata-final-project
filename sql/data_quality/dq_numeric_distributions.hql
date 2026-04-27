DROP TABLE IF EXISTS dq_numeric_distributions_results;

CREATE TABLE dq_numeric_distributions_results
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

SELECT
    'loan_amnt' AS feature,
    CASE
        WHEN loan_amnt IS NULL THEN 'NULL'
        WHEN loan_amnt < 5000 THEN '00000-05000'
        WHEN loan_amnt < 10000 THEN '05000-10000'
        WHEN loan_amnt < 15000 THEN '10000-15000'
        WHEN loan_amnt < 20000 THEN '15000-20000'
        WHEN loan_amnt < 30000 THEN '20000-30000'
        ELSE '30000+'
    END AS bucket,
    COUNT(*) AS count,
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4) AS share_pct
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN loan_amnt IS NULL THEN 'NULL'
        WHEN loan_amnt < 5000 THEN '00000-05000'
        WHEN loan_amnt < 10000 THEN '05000-10000'
        WHEN loan_amnt < 15000 THEN '10000-15000'
        WHEN loan_amnt < 20000 THEN '15000-20000'
        WHEN loan_amnt < 30000 THEN '20000-30000'
        ELSE '30000+'
    END

UNION ALL

SELECT
    'annual_inc',
    CASE
        WHEN annual_inc IS NULL THEN 'NULL'
        WHEN annual_inc < 25000 THEN '000000-025000'
        WHEN annual_inc < 50000 THEN '025000-050000'
        WHEN annual_inc < 75000 THEN '050000-075000'
        WHEN annual_inc < 100000 THEN '075000-100000'
        WHEN annual_inc < 150000 THEN '100000-150000'
        ELSE '150000+'
    END,
    COUNT(*),
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN annual_inc IS NULL THEN 'NULL'
        WHEN annual_inc < 25000 THEN '000000-025000'
        WHEN annual_inc < 50000 THEN '025000-050000'
        WHEN annual_inc < 75000 THEN '050000-075000'
        WHEN annual_inc < 100000 THEN '075000-100000'
        WHEN annual_inc < 150000 THEN '100000-150000'
        ELSE '150000+'
    END

UNION ALL

SELECT
    'dti',
    CASE
        WHEN dti IS NULL THEN 'NULL'
        WHEN dti < 0 THEN '<0'
        WHEN dti < 10 THEN '00-10'
        WHEN dti < 20 THEN '10-20'
        WHEN dti < 30 THEN '20-30'
        WHEN dti < 40 THEN '30-40'
        WHEN dti <= 100 THEN '40-100'
        ELSE '100+'
    END,
    COUNT(*),
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN dti IS NULL THEN 'NULL'
        WHEN dti < 0 THEN '<0'
        WHEN dti < 10 THEN '00-10'
        WHEN dti < 20 THEN '10-20'
        WHEN dti < 30 THEN '20-30'
        WHEN dti < 40 THEN '30-40'
        WHEN dti <= 100 THEN '40-100'
        ELSE '100+'
    END

UNION ALL

SELECT
    'int_rate',
    CASE
        WHEN int_rate IS NULL THEN 'NULL'
        WHEN int_rate < 5 THEN '00-05'
        WHEN int_rate < 10 THEN '05-10'
        WHEN int_rate < 15 THEN '10-15'
        WHEN int_rate < 20 THEN '15-20'
        WHEN int_rate < 25 THEN '20-25'
        ELSE '25+'
    END,
    COUNT(*),
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN int_rate IS NULL THEN 'NULL'
        WHEN int_rate < 5 THEN '00-05'
        WHEN int_rate < 10 THEN '05-10'
        WHEN int_rate < 15 THEN '10-15'
        WHEN int_rate < 20 THEN '15-20'
        WHEN int_rate < 25 THEN '20-25'
        ELSE '25+'
    END

UNION ALL

SELECT
    'revol_util',
    CASE
        WHEN revol_util IS NULL THEN 'NULL'
        WHEN revol_util < 0 THEN '<0'
        WHEN revol_util < 20 THEN '000-020'
        WHEN revol_util < 40 THEN '020-040'
        WHEN revol_util < 60 THEN '040-060'
        WHEN revol_util < 80 THEN '060-080'
        WHEN revol_util <= 100 THEN '080-100'
        ELSE '100+'
    END,
    COUNT(*),
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN revol_util IS NULL THEN 'NULL'
        WHEN revol_util < 0 THEN '<0'
        WHEN revol_util < 20 THEN '000-020'
        WHEN revol_util < 40 THEN '020-040'
        WHEN revol_util < 60 THEN '040-060'
        WHEN revol_util < 80 THEN '060-080'
        WHEN revol_util <= 100 THEN '080-100'
        ELSE '100+'
    END

UNION ALL

SELECT
    'fico_range_low',
    CASE
        WHEN fico_range_low IS NULL THEN 'NULL'
        WHEN fico_range_low < 650 THEN '610-650'
        WHEN fico_range_low < 675 THEN '650-675'
        WHEN fico_range_low < 700 THEN '675-700'
        WHEN fico_range_low < 725 THEN '700-725'
        WHEN fico_range_low < 750 THEN '725-750'
        ELSE '750+'
    END,
    COUNT(*),
    ROUND(COUNT(*) / MAX(total.total_rows) * 100, 4)
FROM base CROSS JOIN total
GROUP BY
    CASE
        WHEN fico_range_low IS NULL THEN 'NULL'
        WHEN fico_range_low < 650 THEN '610-650'
        WHEN fico_range_low < 675 THEN '650-675'
        WHEN fico_range_low < 700 THEN '675-700'
        WHEN fico_range_low < 725 THEN '700-725'
        WHEN fico_range_low < 750 THEN '725-750'
        ELSE '750+'
    END;