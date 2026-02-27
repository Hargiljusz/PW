--generowanie zapytania
SELECT
    'SELECT v.nazwa_kolumny, ROUND((SUM(v.is_missing) * 100.0 / COUNT(*))::numeric, 2) AS procent_brakow ' ||
    'FROM flights_2024 t CROSS JOIN LATERAL ( VALUES ' ||
    STRING_AGG(
        FORMAT('(%L, CASE WHEN t.%I IS NULL OR t.%I::text = '''' THEN 1 ELSE 0 END)', column_name, column_name, column_name),
        ', '
    ) ||
    ') AS v(nazwa_kolumny, is_missing) GROUP BY v.nazwa_kolumny ORDER BY procent_brakow DESC;' AS wygenerowany_kod_sql
FROM information_schema.columns
WHERE table_name = 'flights_2024' AND table_schema = 'public';

--zapytanie korzystające z jsonb
SELECT
    key AS nazwa_kolumny,
    ROUND((SUM(
        CASE 
            -- value #>> '{}' wyciąga wartość jako tekst. 
            -- NULLIF zrównuje pusty string ('') do NULLa.
            WHEN NULLIF(value #>> '{}', '') IS NULL THEN 1 
            ELSE 0 
        END
    ) * 100.0 / COUNT(*))::numeric, 2) AS procent_brakow
FROM
    flights_2024 t
CROSS JOIN LATERAL
    jsonb_each(to_jsonb(t))
GROUP BY
    key
ORDER BY
    procent_brakow DESC;