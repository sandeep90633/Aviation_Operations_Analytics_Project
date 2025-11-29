{{ config(materialized = "view") }}

WITH base AS (
    SELECT 
        *
    FROM {{ ref('stg_departures_base') }}
    WHERE
        status = 'Departed'
        AND codeshare_status = 'IsOperator'
        AND (callsign IS NOT NULL OR aircraft_mode_s IS NOT NULL)
),

-- Assign row numbers by scheduling time so we can pick the best two rows per group
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY flight_date, callsign, aircraft_mode_s, airport_icao
            ORDER BY departure_scheduled_utc
        ) AS rn
    FROM base
),

-- Split into the two sides for self-merge:
primary_record AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
),

secondary_record AS (
    SELECT *
    FROM ranked
    WHERE rn = 2
),

-- different flight_numbers with same callsign and aircraft_mode_s, but one record has correct departure times and another has correct arrival details.
-- check callsigns with ('AFR64JN', 'CFG6ET', 'EAF2085', 'AUA4BF') to understand
merged AS (
    SELECT
        -- Core identifiers
        p.flight_number,
        p.flight_date,
        p.callsign,
        p.status,
        p.codeshare_status,
        p.is_cargo,

        -- Aircraft
        p.aircraft_reg,
        p.aircraft_mode_s,
        p.aircraft_model,

        -- Airline
        p.airline_name,
        COALESCE(
            p.airline_iata,
            LEFT(p.flight_number, CHARINDEX(' ', p.flight_number) - 1)
        ) AS airline_iata,
        COALESCE(
            p.airline_icao,
            LEFT(p.callsign, 3)
        ) AS airline_icao,

        -- Departure airport
        p.airport_icao,

        -- Departure times (merged)
        p.departure_scheduled_utc,
        COALESCE(p.departure_revised_utc, s.departure_revised_utc) AS departure_revised_utc,
        COALESCE(p.departure_runway_utc, s.departure_runway_utc) AS departure_runway_utc,

        -- Arrival airport info
        p.arrival_airport_icao,
        p.arrival_airport_iata,
        p.arrival_airport_name,
        p.arrival_airport_timezone,

        -- Arrival times (merged)
        COALESCE(p.arrival_scheduled_utc, s.arrival_scheduled_utc) AS arrival_scheduled_utc,
        COALESCE(p.arrival_revised_utc, s.arrival_revised_utc) AS arrival_revised_utc,
        COALESCE(p.arrival_runway_utc, s.arrival_runway_utc) AS arrival_runway_utc,

        p.ingestion_timestamp,
        p.data_source
    FROM primary_record p
    LEFT JOIN secondary_record s
        ON p.flight_date = s.flight_date
        AND p.callsign = s.callsign
        AND p.aircraft_mode_s = s.aircraft_mode_s
        AND p.airport_icao = s.airport_icao
)

SELECT 
    *
FROM 
    merged
