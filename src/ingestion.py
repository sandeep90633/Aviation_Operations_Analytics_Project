import sys, os
import logging
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import fetch_opensky_flight_data
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
    
def _ingest_opensky_data(cursor, data, table_name, opensky_columns):
    """Handles table creation and data insertion for a single OpenSky dataset."""
    
    if not data:
        logging.warning(f"Skipping loading, because {table_name} data is empty .")
        return

    logging.info(f"Started ingestion for OpenSky {table_name}...")
    
    # 1. Create Table
    logging.info(f"Creating OpenSky table: {table_name} or checking its existence....")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            icao24 VARCHAR(10),
            firstSeen BIGINT,
            estDepartureAirport VARCHAR(10),
            lastSeen BIGINT,
            estArrivalAirport VARCHAR(10),
            callsign VARCHAR(20),
            estDepartureAirportHorizDistance INT,
            estDepartureAirportVertDistance INT,
            estArrivalAirportHorizDistance INT,
            estArrivalAirportVertDistance INT,
            departureAirportCandidatesCount INT,
            arrivalAirportCandidatesCount INT,
            airport_icao VARCHAR(10),
            record_date DATE,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    logging.info(f"Table {table_name} is created or existed.")

    # 2. Insert Data
    placeholders = ', '.join(['%s'] * len(opensky_columns))
    column_str = ', '.join(opensky_columns)
    
    insert_sql = f"""
        INSERT INTO {table_name} ({column_str})
        VALUES ({placeholders})
    """
    
    logging.info(f"Inserting {len(data)} rows into table '{table_name}'...")
    # Using executemany with placeholders (%s) prevents SQL injection.
    cursor.executemany(insert_sql, data)
    logging.info(f"'{table_name}' data ingestion process finished.")
    
def extract_load_opensky_data(airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, connection):
    
    logging.info("Started OpenSky Network arrival and departure retrieval and loading process.............")
    
    directions = [
        {'name': 'departure', 'endpoint': '/flights/departure', 'table': 'flight_departures'}, 
        {'name': 'arrival', 'endpoint': '/flights/arrival', 'table': 'flight_arrivals'}
    ]
    
    # 1. Fetch Data for both directions
    fetched_data = {}
    for direction in directions:
        logging.info(f"Started retrieval process for {direction['name']}...")
        data, _ = fetch_opensky_flight_data(
            airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, direction['endpoint'], date
        )
        fetched_data[direction['table']] = data
    
    try:
        # Use the transaction context manager to ensure commit/rollback
        with transaction(connection) as cursor:
            for direction in directions:
                table_name = direction['table']
                data = fetched_data[table_name]
                _ingest_opensky_data(cursor, data, table_name, columns)
    
    except Exception as e:
        # Transaction manager handles rollback/logging; re-raise if necessary
        logging.error(f"OpenSky Network data ingestion failed.")
        raise e
        
    logging.info("Completed ingesting and loading both OpenSky arrivals and departures data.")
