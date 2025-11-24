{{ config(
    materialized = "view"
) }}

WITH source as (
    SELECT 
        * 
    FROM 
        {{source('raw_layer', 'airports')}}
),
renamed as (

    SELECT
        -- Surrogate key for airport dimension
        ROW_NUMBER() OVER(ORDER BY icao) as airport_key,
        airport as airport_name,
        country,
        state,
        city,
        UPPER(icao) as airport_icao,
        UPPER(iata) as airport_iata,

        TRY_CAST(elevation_ft::string AS FLOAT) AS elevation_ft,
        TRY_CAST(latitude::string AS FLOAT) AS latitude,
        TRY_CAST(longitude::string AS FLOAT) AS longitude,

        current_timestamp() as record_loaded_at,
        'airports' as source_table

    FROM 
        source
),
deduplicated as (

    SELECT
        *
    FROM
        renamed
    qualify row_number() over (
        partition by airport_icao
        order by record_loaded_at desc
    ) = 1
)

select * from deduplicated