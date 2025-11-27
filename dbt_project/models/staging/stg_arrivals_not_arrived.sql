
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
        status NOT IN ('Arrived', 'Approaching', 'Delayed')
)
SELECT
    *
FROM
    base