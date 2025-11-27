{{ config(
    materialized = "view"
) }}

-- Records that are reliable to connect to flights
WITH base as (
    SELECT
        *
    FROM
        {{ ref('stg_departures_base') }}
    WHERE
        status = 'Departed' and
        codeshare_status = 'IsOperator' and
        (callsign IS NOT NULL or aircraft_mode_s IS NOT NULL)
)
SELECT
    *
FROM
    base
