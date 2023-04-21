WITH sample_data AS (
  SELECT 1 AS id, 10 AS value
  UNION ALL
  SELECT 2, 20
  UNION ALL
  SELECT 3, 30
)
SELECT id, ZIPF(1.5, 100, RANDOM()) AS zipf_value
FROM sample_data;
