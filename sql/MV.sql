-- Zwiększamy pamięć na łączenie tabel do 2 GB
SET work_mem = '2GB';

-- Zwiększamy pamięć na tworzenie widoków i indeksów do 4 GB
SET maintenance_work_mem = '4GB';

-- (Opcjonalnie) Upewniamy się, że silnik chętniej użyje szybkiego Hash Join
SET enable_nestloop = off;


CREATE MATERIALIZED VIEW mv_flights_2026_02_analysis AS
WITH top_airports AS (
	WITH Top30Kraju AS (
	    SELECT "Origin", "OriginState", COUNT(*) as liczba_lotow
	    FROM flights_2024
	    GROUP BY "Origin", "OriginState"
	    ORDER BY liczba_lotow DESC
	    LIMIT 30
	),
	TopWStanach AS (
	    SELECT DISTINCT ON ("OriginState") 
	        "Origin", "OriginState", COUNT(*) as liczba_lotow
	    FROM flights_2024
	    GROUP BY "OriginState", "Origin"
	    ORDER BY "OriginState", liczba_lotow DESC
	)
	
	-- Połączenie obu zbiorów (UNION usunie duplikaty)
	SELECT "Origin", "OriginState", liczba_lotow FROM Top30Kraju
	UNION
	SELECT "Origin", "OriginState", liczba_lotow FROM TopWStanach
	ORDER BY liczba_lotow DESC
)
SELECT 
    f.*, 
    (f."CRSArrTime" / 100)::SMALLINT as CRSArrHour,
    (f."CRSDepTime" / 100)::SMALLINT as CRSDepHour,
    (f."CRSArrTime" % 100)::SMALLINT as CRSArrMinutes,
    (f."CRSDepTime" % 100)::SMALLINT as CRSDepMinutes,

    -- DANE O SAMOLOCIE
    ac.*,
    (2024 - ac.built_year) AS ac_age,

    -- POGODA - WYLOT (Origin)
    wo.tmpf AS origin_tmpf,
    wo.sknt AS origin_sknt,
    wo.gust AS origin_gust,
    wo.p01m AS origin_precip,
    wo.vsby AS origin_vsby,
    wo.is_thunderstorm AS origin_thunderstorm,
    wo.is_snow AS origin_snow,
    wo.is_freezing AS origin_freezing,

    -- POGODA - PRZYLOT (Dest)
    wd.tmpf AS dest_tmpf,
    wd.sknt AS dest_sknt,
    wd.gust AS dest_gust,
    wd.p01m AS dest_precip,
    wd.vsby AS dest_vsby,
    wd.is_thunderstorm AS dest_thunderstorm,
    wd.is_snow AS dest_snow,
    wd.is_freezing AS dest_freezing

FROM 
    flights_2024 f

-- ==========================================
-- FILTR: WYLOT I PRZYLOT TYLKO Z/DO TOP 30
-- Łączymy z naszą wirtualną tabelą z bloku WITH
-- ==========================================
INNER JOIN top_airports top_airports_o ON f."Origin" = top_airports_o."Origin"
INNER JOIN top_airports top_airports_d ON f."Dest" = top_airports_d."Origin"

-- DANE O SAMOLOCIE
LEFT JOIN mv_aircraft_clean ac 
    ON f."Tail_Number" = ac.registration

-- STREFA I POGODA WYLOTU
LEFT JOIN weather_2024 wo 
    ON f."Origin" = wo.station 
    AND f."FlightDate" = wo.local_flight_date 
    AND (
        (f."CRSDepTime" / 100) + 
        CASE WHEN (f."CRSDepTime" % 100) >= 30 THEN 1 ELSE 0 END
    )::SMALLINT = wo.local_hour

LEFT JOIN weather_2024 wd 
    ON f."Dest" = wd.station 
    AND f."FlightDate" = wd.local_flight_date 
    AND (
        (f."CRSArrTime" / 100) + 
        CASE WHEN (f."CRSArrTime" % 100) >= 30 THEN 1 ELSE 0 END
    )::SMALLINT = wd.local_hour;



CREATE INDEX idx_mv_top_flightdate ON mv_flights_2026_02_analysis ("FlightDate");

CREATE INDEX idx_mv_top_origin ON mv_flights_2026_02_analysis ("Origin");

CREATE INDEX idx_mv_top_dest ON mv_flights_2026_02_analysis ("Dest");

CREATE INDEX idx_mv_top_airline ON mv_flights_2026_02_analysis ("Reporting_Airline");

CREATE INDEX idx_mv_top_deptime ON mv_flights_2026_02_analysis ("CRSDepTime");    

CREATE INDEX idx_mv_top_arrtime ON mv_flights_2026_02_analysis ("CRSArrTime"); 

CREATE INDEX "idx_mv_top_FlightDate" ON mv_flights_2026_02_analysis ("FlightDate");

CREATE INDEX "idx_mv_top_Airline" ON mv_flights_2026_02_analysis ("Marketing_Airline_Network");

CREATE INDEX "idx_mv_top_Route" ON mv_flights_2026_02_analysis ("Origin", "Dest");

CREATE INDEX "idx_mv_top_Features" ON mv_flights_2026_02_analysis ("Month", "DayOfWeek", "Origin");

CREATE INDEX "idx_mv_top_ArrDel15" ON mv_flights_2026_02_analysis ("ArrDel15");
               
