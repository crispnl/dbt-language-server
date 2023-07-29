WITH test_data AS (
  SELECT 
    2023 AS year, 
    7 AS month, 
    27 AS day, 
    10 AS hour, 
    00 AS minute, 
    00 AS second,
    3 AS fraction
)
SELECT
  TIMESTAMP_TZ_FROM_PARTS(year, month, day, hour, minute, second),
  TIMESTAMP_TZ_FROM_PARTS(year, month, day, hour, minute, second, fraction),
  TIMESTAMP_TZ_FROM_PARTS(2023, 07, 27, 10, 00, 00),
  TIMESTAMP_TZ_FROM_PARTS(2023, 07, 27, 10, 00, 00, 3),
  TIMESTAMP_TZ_FROM_PARTS(1, 2, 3, 4, 5, 6),
  TIMESTAMP_TZ_FROM_PARTS(1.1, 2.2, 3.3, 4.4, 5.5, 6.6),
  TIMESTAMP_TZ_FROM_PARTS('1', '2', '3', '4', '5', '6'),

  TIMESTAMP_TZ_FROM_PARTS(DATE('2023-07-27'), TO_TIME('10:00:00')),
  TIMESTAMP_TZ_FROM_PARTS('2023-07-27', '10:00:00'),
  TIMESTAMP_TZ_FROM_PARTS(DATE_FROM_PARTS(2023, 07, 27), TIME_FROM_PARTS(10, 30, 00)),
  TIMESTAMP_TZ_FROM_PARTS(CURRENT_DATE(), CURRENT_TIME()),
  TIMESTAMP_TZ_FROM_PARTS('2222-02-02T12:34:56-07:00'::timestamp, '2023-06-19T11:11:11-11:11'::timestamp),
  
  TIMESTAMP_TZ_FROM_PARTS(2013, 4, 5, 12, 00, 00, 0, 'America/Los_Angeles')
FROM test_data;
