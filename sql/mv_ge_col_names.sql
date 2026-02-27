SELECT a.attname AS column_name,
       format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_attribute a
JOIN pg_class t ON a.attrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE t.relname = 'mv_flights_2026_02_analysis'
  AND a.attnum > 0                      
  AND NOT a.attisdropped                 
ORDER BY a.attnum;