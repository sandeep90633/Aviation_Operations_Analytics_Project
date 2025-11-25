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
        
        -- Departure Logistics
        departure_terminal,
        departure_runway,

        -- Arrival Information (Destination)
        
        -- Arrival Times - casting time strings to proper timestamps
        arrival_scheduledtime_utc AS arrival_scheduled_utc,
        arrival_scheduledtime_local AS arrival_scheduled_local,
        arrival_revisedtime_utc AS arrival_revised_utc,
        arrival_revisedtime_local AS arrival_revised_local,
        arrival_runwaytime_utc AS arrival_runway_utc,
        arrival_runwaytime_local AS arrival_runway_local,
        
        -- Arrival Logistics
        arrival_terminal,
        arrival_runway,
        arrival_gate,
        arrival_baggagebelt AS arrival_baggage_belt,

        ingestion_timestamp,
        data_source
    FROM
        source
    WHERE 
        isCargo = FALSE
        AND (
            status <> 'IsOperator'
            OR (callSign IS NOT NULL AND aircraft_modeS IS NOT NULL)
        )
),
deduplicated AS (
    SELECT
        *
    FROM
        filtered
    qualify row_number() over (
        partition by flight_number, flight_date, departure_airport_icao, departure_scheduled_utc, airport_icao, arrival_scheduled_utc
        order by flight_date desc
    ) = 1 
)

SELECT * FROM deduplicated