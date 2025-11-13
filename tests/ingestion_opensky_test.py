import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import fetch_opensky_flight_data
from common_scripts.aerodatabox_connection import fetch_aerodatabox_data
from snowflake_handler import SnowflakeHandler
from utils.logging import setup_logger

logger = setup_logger('opensky_arrival.log')

def main():
    
    snowflake_handler = SnowflakeHandler("credentials", "snowflake_configs.json" )
    
    if not snowflake_handler.conn:
        snowflake_handler.connect()
        
    cursor = snowflake_handler.conn.cursor()
    
    retrieve_airports = "SELECT DISTINCT icao FROM airports"
    cursor.execute(retrieve_airports)
    
    # Fetch all rows (each row is a tuple)
    rows = cursor.fetchall()

    # Extract only the airport values into a list
    airports = [row[0] for row in rows]

    airports_icao = ['EDDN', 'EDDF', 'EDDM']

    API_BASE_URL = "https://opensky-network.org/api"

    columns = [
        'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen',
        'estArrivalAirport', 'callsign',
        'estDepartureAirportHorizDistance', 'estDepartureAirportVertDistance',
        'estArrivalAirportHorizDistance', 'estArrivalAirportVertDistance',
        'departureAirportCandidatesCount', 'arrivalAirportCandidatesCount', 'airport_icao', 'ingestion_timestamp'
    ]
    
    directions = ['departure', 'arrival']
    
    for direction in directions:
        
        data, columns = fetch_opensky_flight_data(airports_icao, columns, "credentials/opensky_credentials.json", API_BASE_URL, f"/flights/{direction}", "2025-01-02")
        
        cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS flight_{direction}s (
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
                    ingestion_timestamp VARCHAR(40)
                )
                """)
            
        logger.info(f"Table flights_{direction}s is created or existed.")
        
        insert_sql = f"""
        INSERT INTO flight_{direction}s (
            icao24,
            firstSeen,
            estDepartureAirport,
            lastSeen,
            estArrivalAirport,
            callsign,
            estDepartureAirportHorizDistance,
            estDepartureAirportVertDistance,
            estArrivalAirportHorizDistance,
            estArrivalAirportVertDistance,
            departureAirportCandidatesCount,
            arrivalAirportCandidatesCount,
            airport_icao,
            ingestion_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
        cursor.executemany(insert_sql, data)

        logger.info(f"'{direction}' data ingestion process finished.")
    
if __name__ == "__main__":
    main()