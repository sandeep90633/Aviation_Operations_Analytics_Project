{{ config(
    materialized = "view"
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{source('raw_layer', 'airport_arrivals')}}
),
-- Exclude records where iscargo = TRUE.
-- If status = 'IsOperator', then both callsign and aircraft_modes must be NOT NULL.
filtered AS (
    SELECT
        *
    FROM
        source
    WHERE 
        iscargo = FALSE
        AND (
            status <> 'IsOperator'
            OR (callsign IS NOT NULL AND aircraft_modes IS NOT NULL)
        )
),
deduplicated AS (
    SELECT
        *
    FROM
        filtered
    qualify row_number() over (
        partition by flight_number, flight_date, departure_airport_icao, departure_scheduledtime_utc, airport_icao, arrival_scheduledtime_utc
        order by flight_date desc
    ) = 1 
)

SELECT * FROM deduplicated