{{ config(
    materialized = "view"
) }}

-- Records that are unable to connect to flights
WITH base as (
    SELECT
        *
    FROM
        {{ ref('stg_departures_base') }}
    WHERE
        codeshare_status = 'IsOperator' and 
        status = 'Departed' and 
        (callsign IS NULL and aircraft_mode_s IS NULL)
)
SELECT
    *
FROM
    base