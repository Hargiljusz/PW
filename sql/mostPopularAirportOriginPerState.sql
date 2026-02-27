SELECT DISTINCT ON ("OriginState") 
    "OriginState", 
    "Origin", 
    COUNT(*) as liczba_lotow
FROM flights_2024
WHERE "Origin" IS NOT NULL AND "OriginState" IS NOT NULL
GROUP BY "OriginState", "Origin"
ORDER BY "OriginState", COUNT(*) DESC;