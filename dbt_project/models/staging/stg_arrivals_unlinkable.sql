{{ config(
    materialized = "view"
) }}

-- Records that are unable to connect to flights
WITH base as (
    SELECT
        *
    FROM
        {{ ref('stg_arrivals_base') }}
    WHERE
        codeshare_status = 'IsOperator' and
        status IN ('Arrived','Approaching','Delayed') and 
        (callsign IS NULL and aircraft_mode_s IS NULL)
)
SELECT
    *
FROM
    base