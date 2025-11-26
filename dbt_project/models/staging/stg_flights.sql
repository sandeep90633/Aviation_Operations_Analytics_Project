{{ config(
    materialized = "view"
) }}

WITH source as (
    SELECT
        *
    FROM
        {{ source('raw_layer', 'flights') }}
),
renamed as (
    SELECT
        upper(icao24) as icao24,
        upper(TRIM(callsign)) as callsign,
        upper(estdepartureairport) as est_departure_airport,
        upper(estarrivalairport) as est_arrival_airport,
        try_cast(firstseen as number) as first_seen,
        try_cast(lastseen as number) as last_seen,
        to_timestamp(try_cast(firstseen as number)) as first_seen_ts,
        to_timestamp(try_cast(lastseen as number)) as last_seen_ts,
        try_cast(estdepartureairporthorizdistance as number) as dep_airport_horiz_distance,
        try_cast(estdepartureairportvertdistance as number) as dep_airport_vert_distance,
        try_cast(estarrivalairporthorizdistance as number) as arr_airport_horiz_distance,
        try_cast(estarrivalairportvertdistance as number) as arr_airport_vert_distance,
        try_cast(departureairportcandidatescount as number) as departure_airport_candidates_count,
        try_cast(arrivalairportcandidatescount as number) as arrival_airport_candidates_count,
        try_cast(record_date as date) as flight_date,
        ingestion_timestamp,
    FROM
        source
),

deduplicated as (

    SELECT 
        *
    FROM 
        renamed
    qualify row_number() over (
        partition by icao24, first_seen_ts
        order by ingestion_timestamp desc
    ) = 1
)

select * from deduplicated