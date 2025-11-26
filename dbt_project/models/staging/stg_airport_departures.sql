{{ config(
    materialized = "view"
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{source('raw_layer', 'airport_departures')}}
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
        arrival_airport_icao,
        arrival_airport_iata,
        arrival_airport_name,
        arrival_airport_timezone,
        
        -- Arrival Times - casting time strings to proper timestamps
        arrival_scheduledtime_utc AS arrival_scheduled_utc,
        arrival_scheduledtime_local AS arrival_scheduled_local,
        arrival_revisedtime_utc AS arrival_revised_utc,
        arrival_revisedtime_local AS arrival_revised_local,
        arrival_runwaytime_utc AS arrival_runway_utc,
        arrival_runwaytime_local AS arrival_runway_local,
        
        -- Arrival Logistics
        arrival_terminal,
        arrival_gate,
        arrival_baggagebelt AS arrival_baggage_belt,

        --flags
        CASE WHEN departure_revisedtime_utc IS NULL THEN 1 ELSE 0 END as is_missing_revised_time,
        CASE WHEN departure_runwaytime_utc IS NULL THEN 1 ELSE 0 END as is_missing_runway_time,
        CASE WHEN callSign IS NULL THEN 1 ELSE 0 END as is_missing_callsign,
        CASE WHEN aircraft_modeS IS NULL THEN 1 ELSE 0 END as is_missing_aircraft_mode_s,

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
latest_records AS (
    -- keeping the latest record per full flight identity
    SELECT 
        *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
            flight_number,
            flight_date,
            airport_icao,
            departure_scheduled_utc,
            arrival_airport_icao,
            arrival_scheduled_utc
        ORDER BY ingestion_timestamp DESC
    ) = 1
),

final_dedup AS (
    -- dedupe further using aircraft + callsign identifiers
    SELECT *
    FROM latest_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
            flight_number,
            flight_date,
            callsign,
            aircraft_mode_s,
            airport_icao
        ORDER BY 
            -- Push records with BOTH nulls to end (so they are dropped)
            CASE 
                WHEN departure_revised_utc IS NULL 
                 AND arrival_scheduled_utc IS NULL 
                THEN 1 ELSE 0 
            END,
            -- If both options valid, keep earliest scheduled departure
            departure_scheduled_utc
    ) = 1
)

SELECT * FROM final_dedup