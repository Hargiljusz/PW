-- Sprawdzenie brakujących dopasowań
SELECT 
    f."Marketing_Airline_Network" AS airline,
    COUNT(*) AS total_flights,
    COUNT(a.registration) AS matched_flights,
    COUNT(*) - COUNT(a.registration) AS missing_aircraft,
    ROUND((COUNT(*) - COUNT(a.registration))::numeric / COUNT(*) * 100, 2) AS missing_percentage
FROM flights_2024 f
LEFT JOIN aircrafts a ON f."Tail_Number" = a.registration
WHERE f."Tail_Number" IS NOT NULL
GROUP BY f."Marketing_Airline_Network"
ORDER BY missing_percentage DESC;


-- Sprawdzenie nie dopasowanych samolotów, braki w bazie open sky
SELECT DISTINCT f."Tail_Number"
FROM flights_2024 f
LEFT JOIN aircrafts a ON f."Tail_Number" = a.registration
WHERE a.registration IS NULL 
  AND f."Tail_Number" IS NOT NULL
LIMIT 10;