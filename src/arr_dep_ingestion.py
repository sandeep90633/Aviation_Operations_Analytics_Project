import sys, os
import logging
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.aerodatabox_connection import fetch_aerodatabox_data

# This ensures atomicity: all inserts commit, or all rollback on error.
@contextmanager
def transaction(conn):
    """Context manager for database transactions."""
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Transaction failed, rolling back: {e}")
        raise
    
def _ingest_aerodatabox_data(cursor, data, table_name, column_names, specific_cols_sql):
    """Handles table creation and data insertion for a single AeroDataBox dataset."""
    if not data:
        logging.warning(f"Skipping loading, because {table_name} data is empty.")
        return

    # Base columns common to both AeroDataBox tables
    base_columns_sql = """
        flight_number VARCHAR(20), flight_date DATE NOT NULL, callsign VARCHAR(20),
        status VARCHAR(50), iscargo BOOLEAN, aircraft_reg VARCHAR(20),
        aircraft_modeS VARCHAR(20), aircraft_model VARCHAR(100), airline_name VARCHAR(100),
        airline_iata VARCHAR(10), airline_icao VARCHAR(10), airport_icao VARCHAR(10) NOT NULL,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(2),
        data_source VARCHAR(50) DEFAULT 'AeroDataBox'
    """

    # Create Table
    logging.info(f"Creating AeroDataBox table: {table_name} or checking its existence.....")
    # Using an f-string for table/column names is generally safe here as they are controlled internally.
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({base_columns_sql}, {specific_cols_sql})
    """
    cursor.execute(create_table_query)
    logging.info(f"Created {table_name} table or it already existed.")

    # Insert Data
    placeholders = ', '.join(['%s'] * len(column_names))
    column_str = ', '.join(column_names)
    insert_query = f"""
        INSERT INTO {table_name} ({column_str})
        VALUES ({placeholders})
    """

    logging.info(f"Loading {len(data)} rows into {table_name}.....")
    # Using executemany with placeholders (%s) prevents SQL injection.
    cursor.executemany(insert_query, data)
    logging.info(f"Finished {table_name} data ingestion process.")
    
def extract_load_aerodatabox_data(aerodatabox_api_key_path, BASE_URL, endpoint, airports_icao, date, connection):
    
    logging.info("Started AeroDataBox arrivals and departures retrieval and loading process.............")
    
    # Fetch Data
    _, _, departures, arrivals = fetch_aerodatabox_data(
        aerodatabox_api_key_path, BASE_URL, endpoint, airports_icao, date
    )

    # Define column details for Departures
    adbox_departure_cols_sql = """
        departure_scheduledtime_utc TIMESTAMP, departure_scheduledtime_local TIMESTAMP,
        departure_revisedtime_utc TIMESTAMP, departure_revisedtime_local TIMESTAMP,
        departure_runwaytime_utc TIMESTAMP, departure_runwaytime_local TIMESTAMP,
        departure_terminal VARCHAR(10), departure_runway VARCHAR(10), 
        arrival_airport_icao VARCHAR(10), arrival_airport_iata VARCHAR(10), 
        arrival_airport_name VARCHAR(100), arrival_airport_timezone VARCHAR(50),
        arrival_scheduledtime_utc TIMESTAMP, arrival_scheduledtime_local TIMESTAMP,
        arrival_revisedtime_utc TIMESTAMP, arrival_revisedtime_local TIMESTAMP,
        arrival_runwaytime_utc TIMESTAMP, arrival_runwaytime_local TIMESTAMP,
        arrival_terminal VARCHAR(10), arrival_gate VARCHAR(10), arrival_baggagebelt VARCHAR(20)
    """
    departure_cols = [
        # Common
        'flight_number', 'flight_date', 'callsign', 'status', 'iscargo', 'aircraft_reg', 'aircraft_modeS', 'aircraft_model',
        'airline_name', 'airline_iata', 'airline_icao', 'airport_icao',
        # Departure Specific
        'departure_scheduledtime_utc', 'departure_scheduledtime_local', 'departure_revisedtime_utc', 'departure_revisedtime_local',
        'departure_runwaytime_utc', 'departure_runwaytime_local', 'departure_terminal', 'departure_runway',
        # Arrival Info (destination)
        'arrival_airport_icao', 'arrival_airport_iata', 'arrival_airport_name', 'arrival_airport_timezone',
        'arrival_scheduledtime_utc', 'arrival_scheduledtime_local', 'arrival_revisedtime_utc', 'arrival_revisedtime_local',
        'arrival_runwaytime_utc', 'arrival_runwaytime_local', 'arrival_terminal', 'arrival_gate', 'arrival_baggagebelt'
    ]

    # Define column details for Arrivals
    adbox_arrival_cols_sql = """
        departure_airport_icao VARCHAR(10), departure_airport_iata VARCHAR(10), 
        departure_airport_name VARCHAR(100), departure_airport_timezone VARCHAR(50),
        departure_scheduledtime_utc TIMESTAMP, departure_scheduledtime_local TIMESTAMP,
        departure_revisedtime_utc TIMESTAMP, departure_revisedtime_local TIMESTAMP,
        departure_runwaytime_utc TIMESTAMP, departure_runwaytime_local TIMESTAMP,
        departure_terminal VARCHAR(10), departure_runway VARCHAR(10),
        arrival_scheduledtime_utc TIMESTAMP, arrival_scheduledtime_local TIMESTAMP,
        arrival_revisedtime_utc TIMESTAMP, arrival_revisedtime_local TIMESTAMP,
        arrival_runwaytime_utc TIMESTAMP, arrival_runwaytime_local TIMESTAMP,
        arrival_terminal VARCHAR(10), arrival_runway VARCHAR(10), 
        arrival_gate VARCHAR(10), arrival_baggagebelt VARCHAR(20)
    """
    arrival_cols = [
        # Common
        'flight_number', 'flight_date', 'callsign', 'status', 'iscargo', 'aircraft_reg', 'aircraft_modeS', 'aircraft_model',
        'airline_name', 'airline_iata', 'airline_icao', 'airport_icao',
        # Departure Info (origin)
        'departure_airport_icao', 'departure_airport_iata', 'departure_airport_name', 'departure_airport_timezone',
        'departure_scheduledtime_utc', 'departure_scheduledtime_local', 'departure_revisedtime_utc', 'departure_revisedtime_local',
        'departure_runwaytime_utc', 'departure_runwaytime_local', 'departure_terminal', 'departure_runway',
        # Arrival Specific
        'arrival_scheduledtime_utc', 'arrival_scheduledtime_local', 'arrival_revisedtime_utc', 'arrival_revisedtime_local',
        'arrival_runwaytime_utc', 'arrival_runwaytime_local', 'arrival_terminal', 'arrival_runway', 'arrival_gate', 'arrival_baggagebelt'
    ]
    
    # Ingest Data within a Transaction
    try:
        with transaction(connection) as cursor:
            _ingest_aerodatabox_data(cursor, departures, 'airport_departures', departure_cols, adbox_departure_cols_sql)
            _ingest_aerodatabox_data(cursor, arrivals, 'airport_arrivals', arrival_cols, adbox_arrival_cols_sql)

    except Exception as e:
        # Transaction manager handles rollback/logging; re-raise if necessary
        logging.error(f"AeroDataBox data ingestion failed.")
        raise e
        
    logging.info("Completed ingesting both AeroDataBox arrivals and departures data.")