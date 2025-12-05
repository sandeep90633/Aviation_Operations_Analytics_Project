{{ config(
    materialized = "view"
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{source('raw_layer', 'airport_arrivals')}}
),
-- Exclude records where iscargo = TRUE and not the flights that have different scheduled date to flight_date
filtered AS (
    SELECT
        -- Core Flight Identifiers
        number AS flight_number,
        TRY_CAST(flight_date AS DATE) AS flight_date,
        UPPER(callSign) AS callsign,
        TRIM(status) AS status,
        TRIM(codeshareStatus) AS codeshare_status,
        isCargo AS is_cargo,

        -- Aircraft Information
        aircraft_reg,
        aircraft_modeS AS aircraft_mode_s,
        aircraft_model,

        -- Airline Information
        airline_name,
        airline_iata,
        airline_icao,

        -- Airport Information (Focus Airport - assuming this is the reference airport for the row)
        airport_icao,

        -- Departure Information (Origin)
        departure_airport_icao,
        departure_airport_iata,
        departure_airport_name,
        departure_airport_timezone,
        
        -- Departure Times - casting time strings to proper timestamps
        departure_scheduledtime_utc AS departure_scheduled_utc,
        departure_scheduledtime_local AS departure_scheduled_local,
        departure_revisedtime_utc AS departure_revised_utc,
        departure_revisedtime_local AS departure_revised_local,
        departure_runwaytime_utc AS departure_runway_utc,
        departure_runwaytime_local AS departure_runway_local,
        
        -- Arrival Times - casting time strings to proper timestamps
        arrival_scheduledtime_utc AS arrival_scheduled_utc,
        arrival_scheduledtime_local AS arrival_scheduled_local,
        arrival_revisedtime_utc AS arrival_revised_utc,
        arrival_revisedtime_local AS arrival_revised_local,
        arrival_runwaytime_utc AS arrival_runway_utc,
        arrival_runwaytime_local AS arrival_runway_local,

        ingestion_timestamp,
        data_source
    FROM
        source
    WHERE 
        isCargo = FALSE
        AND DATE(arrival_scheduledtime_utc) = flight_date
),
latest_records AS (
    -- keeping the latest record per full flight identity
    SELECT 
        *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
            flight_number,
            flight_date,
            departure_airport_icao,
            departure_scheduled_utc,
            airport_icao,
            arrival_scheduled_utc
        ORDER BY ingestion_timestamp DESC
    ) = 1
)

SELECT * FROM latest_records