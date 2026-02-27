-- 1. Rozmiar samej tabeli (bez indeksów)
SELECT pg_size_pretty(pg_relation_size('nazwa_tabeli'));

--2. Rozmiar całkowity (tabela + indeksy)
SELECT pg_size_pretty(pg_total_relation_size('nazwa_tabeli'));

--3. Szczegółowy podział (tabela vs indeksy)
SELECT 
    relname AS "Tabela",
    pg_size_pretty(pg_table_size(C.oid)) AS "Rozmiar tabeli",
    pg_size_pretty(pg_indexes_size(C.oid)) AS "Rozmiar indeksów",
    pg_size_pretty(pg_total_relation_size(C.oid)) AS "Suma"
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND relkind = 'r'
  AND relname = 'nazwa_tabeli';

--Funkcja	Co mierzy?
--pg_relation_size	Tylko główny plik danych tabeli (bez indeksów i TOAST).
--pg_table_size	Tabela + dane TOAST (dla dużych pól tekstowych/binarnych).
--pg_indexes_size	Sumaryczny rozmiar wszystkich indeksów przypiętych do tabeli.
--pg_total_relation_size	Wszystko: tabela + indeksy + TOAST.

SELECT pg_size_pretty(pg_total_relation_size('nazwa_twojego_widoku'));



SELECT 
    relname AS "Obiekt",
    CASE 
        WHEN relkind = 'r' THEN 'Tabela'
        WHEN relkind = 'm' THEN 'Zmatrializowany Widok'
        WHEN relkind = 'v' THEN 'Widok'
    END AS "Typ",
    pg_size_pretty(pg_table_size(C.oid)) AS "Rozmiar tabeli",
    pg_size_pretty(pg_indexes_size(C.oid)) AS "Rozmiar indeksów",
    pg_size_pretty(pg_total_relation_size(C.oid)) AS "Suma"
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND relkind IN ('r', 'm') -- m to widok zmatrializowany, r - tabela, v -widok
ORDER BY pg_total_relation_size(C.oid) DESC; 