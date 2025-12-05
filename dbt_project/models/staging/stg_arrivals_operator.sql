{{ config(materialized='view') }}

WITH base AS (
    SELECT
        *,
        CASE 
            WHEN arrival_runway_utc IS NOT NULL THEN 2
            WHEN arrival_revised_utc IS NOT NULL THEN 1
            ELSE 0
        END AS reliability_score
    FROM {{ ref('stg_arrivals_base') }}
    WHERE status IN ('Arrived','Approaching','Delayed')
      AND (callsign IS NOT NULL OR aircraft_mode_s IS NOT NULL)
      AND codeshare_status = 'IsOperator'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY flight_date, callsign, aircraft_mode_s, airport_icao
            ORDER BY reliability_score DESC
        ) AS rn
    FROM base
),

primary AS (
    SELECT * FROM ranked WHERE rn = 1
),

secondary AS (
    SELECT * FROM ranked WHERE rn = 2
),
merged AS (
    SELECT
        p.flight_number,
        p.flight_date,
        p.callsign,
        p.status,
        p.codeshare_status,
        p.is_cargo,

        p.aircraft_reg,
        p.aircraft_mode_s,
        p.aircraft_model,

        /* Airline info - fallback if missing */
        COALESCE(p.airline_name, s.airline_name) AS airline_name,
        COALESCE(
            p.airline_iata,
            LEFT(p.flight_number, CHARINDEX(' ', p.flight_number) - 1)
        ) AS airline_iata,
        COALESCE(
            p.airline_icao,
            LEFT(p.callsign, 3)
        ) AS airline_icao,

        p.airport_icao,

        /* Arrival times */
        COALESCE(p.arrival_scheduled_utc, s.arrival_scheduled_utc) AS arrival_scheduled_utc,
        COALESCE(p.arrival_revised_utc, s.arrival_revised_utc) AS arrival_revised_utc,
        COALESCE(p.arrival_runway_utc, s.arrival_runway_utc) AS arrival_runway_utc,
        
        -- Departure airport info
        p.departure_airport_icao,
        p.departure_airport_iata,
        p.departure_airport_name,
        p.departure_airport_timezone,
        
        /* Departure times for context - merged logically */
        COALESCE(p.departure_scheduled_utc, s.departure_scheduled_utc) AS departure_scheduled_utc,
        COALESCE(p.departure_revised_utc, s.departure_revised_utc) AS departure_revised_utc,
        COALESCE(p.departure_runway_utc, s.departure_runway_utc) AS departure_runway_utc,

        p.ingestion_timestamp,
        p.data_source

    FROM primary p
    LEFT JOIN secondary s
        ON p.flight_date = s.flight_date
        AND p.callsign = s.callsign
        AND p.aircraft_mode_s = s.aircraft_mode_s
        AND p.airport_icao = s.airport_icao
)
SELECT
    *
FROM
    merged