CREATE OR REPLACE VIEW data_daily_counts AS
SELECT created_date, SUM(value) AS value
FROM (
    SELECT created_date, value
    FROM data_daily_counts_cached
  UNION ALL
    SELECT created_date, diff AS value
    FROM data_daily_counts_queue
) combine
GROUP BY created_date;
