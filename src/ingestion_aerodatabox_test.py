import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import fetch_opensky_flight_data
from common_scripts.aerodatabox_connection import fetch_aerodatabox_data
from snowflake_handler import SnowflakeHandler
from utils.logging import setup_logger

logger = setup_logger('aerodatabox_data_loader.log')

def main():
    
    snowflake_handler = SnowflakeHandler("credentials", "snowflake_configs.json" )
    
    if not snowflake_handler.conn:
        snowflake_handler.connect()
        
    cursor = snowflake_handler.conn.cursor()
    
    # retrieve_airports = "SELECT DISTINCT icao FROM airports"
    # cursor.execute(retrieve_airports)
    
    # # Fetch all rows (each row is a tuple)
    # rows = cursor.fetchall()

    # # Extract only the airport values into a list
    # airports = [row[0] for row in rows]

    airports_icao = ['EDDN']
    
    BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"
    endpoint = "flights/airports/"
    
    arrival_columns, departures, arrivals = fetch_aerodatabox_data("credentials/aerodatabox_api_key.json", BASE_URL, endpoint, airports_icao, "2025-01-02")
    
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS airport_departures (
                        flight_number VARCHAR(20),
                        flight_date DATE NOT NULL,
                        callsign VARCHAR(20),
                        status VARCHAR(50),
                        iscargo BOOLEAN,
                        aircraft_reg VARCHAR(20),
                        aircraft_modeS VARCHAR(20),
                        aircraft_model VARCHAR(100),
                        airline_name VARCHAR(100),
                        airline_iata VARCHAR(10),
                        airline_icao VARCHAR(10),
                        airport_icao VARCHAR(10) NOT NULL,
                        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                        data_source VARCHAR(50) DEFAULT 'AeroDataBox',

                        departure_scheduledtime_utc TIMESTAMP,
                        departure_scheduledtime_local TIMESTAMP,
                        departure_revisedtime_utc TIMESTAMP,
                        departure_revisedtime_local TIMESTAMP,
                        departure_runwaytime_utc TIMESTAMP,
                        departure_runwaytime_local TIMESTAMP,
                        departure_terminal VARCHAR(10),
                        departure_runway VARCHAR(10),
                        departure_quality VARIANT,

                        arrival_airport_icao VARCHAR(10),
                        arrival_airport_iata VARCHAR(10),
                        arrival_airport_name VARCHAR(100),
                        arrival_airport_timezone VARCHAR(50),
                        arrival_scheduledtime_utc TIMESTAMP,
                        arrival_scheduledtime_local TIMESTAMP,
                        arrival_revisedtime_utc TIMESTAMP,
                        arrival_revisedtime_local TIMESTAMP,
                        arrival_runwaytime_utc TIMESTAMP,
                        arrival_runwaytime_local TIMESTAMP,
                        arrival_terminal VARCHAR(10),
                        arrival_gate VARCHAR(10),
                        arrival_baggagebelt VARCHAR(20),
                        arrival_quality VARIANT
                    )
                """    
                )
    
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS airport_arrivals (
                        flight_number VARCHAR(20),
                        flight_date DATE NOT NULL,
                        callsign VARCHAR(20),
                        status VARCHAR(50),
                        iscargo BOOLEAN,
                        aircraft_reg VARCHAR(20),
                        aircraft_modeS VARCHAR(20),
                        aircraft_model VARCHAR(100),
                        airline_name VARCHAR(100),
                        airline_iata VARCHAR(10),
                        airline_icao VARCHAR(10),
                        airport_icao VARCHAR(10) NOT NULL,
                        
                        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                        data_source VARCHAR(50) DEFAULT 'AeroDataBox',

                        departure_airport_icao VARCHAR(10),
                        departure_airport_iata VARCHAR(10),
                        departure_airport_name VARCHAR(100),
                        departure_airport_timezone VARCHAR(50),
                        departure_scheduledtime_utc TIMESTAMP,
                        departure_scheduledtime_local TIMESTAMP,
                        departure_revisedtime_utc TIMESTAMP,
                        departure_revisedtime_local TIMESTAMP,
                        departure_runwaytime_utc TIMESTAMP,
                        departure_runwaytime_local TIMESTAMP,
                        departure_terminal VARCHAR(10),
                        departure_runway VARCHAR(10),
                        departure_quality VARIANT,

                        arrival_scheduledtime_utc TIMESTAMP,
                        arrival_scheduledtime_local TIMESTAMP,
                        arrival_revisedtime_utc TIMESTAMP,
                        arrival_revisedtime_local TIMESTAMP,
                        arrival_runwaytime_utc TIMESTAMP,
                        arrival_runwaytime_local TIMESTAMP,
                        arrival_terminal VARCHAR(10),
                        arrival_runway VARCHAR(10),
                        arrival_gate VARCHAR(10),
                        arrival_baggagebelt VARCHAR(20),
                        arrival_quality VARIANT
                    )
                   """)
    
    INSERT_DEPARTURES_QUERY = """
                    INSERT INTO airport_departures (
                        flight_number, flight_date, callsign, status, iscargo,
                        aircraft_reg, aircraft_modeS, aircraft_model,
                        airline_name, airline_iata, airline_icao, airport_icao,
                        
                        ingestion_timestamp, data_source,

                        departure_scheduledtime_utc, departure_scheduledtime_local,
                        departure_revisedtime_utc, departure_revisedtime_local,
                        departure_runwaytime_utc, departure_runwaytime_local,
                        departure_terminal, departure_runway, departure_quality,

                        arrival_airport_icao, arrival_airport_iata, arrival_airport_name, arrival_airport_timezone,
                        arrival_scheduledtime_utc, arrival_scheduledtime_local,
                        arrival_revisedtime_utc, arrival_revisedtime_local,
                        arrival_runwaytime_utc, arrival_runwaytime_local,
                        arrival_terminal, arrival_gate, arrival_baggagebelt, arrival_quality
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,

                        %s, %s,

                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,

                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
                    )
                """
    INSERT_ARRIVALS_QUERY = """
                    INSERT INTO airport_arrivals (
                        flight_number, flight_date, callsign, status, iscargo,
                        aircraft_reg, aircraft_modeS, aircraft_model,
                        airline_name, airline_iata, airline_icao, airport_icao,
                        
                        ingestion_timestamp, data_source,

                        departure_airport_icao, departure_airport_iata, departure_airport_name, departure_airport_timezone,
                        departure_scheduledtime_utc, departure_scheduledtime_local,
                        departure_revisedtime_utc, departure_revisedtime_local,
                        departure_runwaytime_utc, departure_runwaytime_local,
                        departure_terminal, departure_runway, departure_quality,

                        arrival_scheduledtime_utc, arrival_scheduledtime_local,
                        arrival_revisedtime_utc, arrival_revisedtime_local,
                        arrival_runwaytime_utc, arrival_runwaytime_local,
                        arrival_terminal, arrival_runway, arrival_gate, arrival_baggagebelt, arrival_quality
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,

                        %s, %s,

                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,

                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s
                    )
                """


    cursor.executemany(INSERT_ARRIVALS_QUERY, arrivals)

    logger.info("Finished arrivals data ingestion process finished.")
    
    cursor.executemany(INSERT_DEPARTURES_QUERY, departures)
    
    logger.info("Finished departures data ingestion process finished.")
    
if __name__ == "__main__":
    main()