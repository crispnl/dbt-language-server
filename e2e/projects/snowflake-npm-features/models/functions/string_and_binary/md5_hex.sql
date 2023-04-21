WITH sample_data AS (
  SELECT 1 AS id, 'Hello world' AS string
  UNION ALL
  SELECT 2, 'Snowflakes for Snowflakes'
  UNION ALL
  SELECT 3, 'If you read this, you have reached your success'
)
SELECT id, MD5_HEX(string) AS md5_hex_value
FROM sample_data;
