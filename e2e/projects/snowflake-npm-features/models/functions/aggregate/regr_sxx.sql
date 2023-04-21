WITH sample_data AS (
    SELECT 1 AS x, 2 AS y
    UNION ALL
    SELECT 2, 4
    UNION ALL
    SELECT 3, 6
    UNION ALL
    SELECT 4, 8
    UNION ALL
    SELECT 5, 10
    UNION ALL
    SELECT 6, 12
    UNION ALL
    SELECT 7, 14
    UNION ALL
    SELECT 8, 16
    UNION ALL
    SELECT 9, 18
    UNION ALL
    SELECT 10, 20
)
SELECT REGR_SXX(x, y) AS regr_sxx_result FROM sample_data;