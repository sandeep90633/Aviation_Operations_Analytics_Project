from src.flights_ingestion import extract_load_opensky_data
from src.arr_dep_ingestion import extract_load_aerodatabox_data
from utils.logging import setup_logger
from snowflake_handler import SnowflakeHandler

logger = setup_logger('opensky_ingestion.log')

def main():
    
    columns = [
        'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen',
        'estArrivalAirport', 'callsign', 'estDepartureAirportHorizDistance',
        'estDepartureAirportVertDistance', 'estArrivalAirportHorizDistance',
        'estArrivalAirportVertDistance', 'departureAirportCandidatesCount',
        'arrivalAirportCandidatesCount', 'record_date'
    ]
    
    opensky_cred_file = "credentials/opensky_credentials.json"
    
    OPENSKY_API_BASE_URL = "https://opensky-network.org/api"
    
    date = "2025-01-02"
    
    snowflake_handler = SnowflakeHandler()
    connection = snowflake_handler.conn
    
    if not connection:
        logger.info("Connecting to Snowflake...")
        snowflake_handler.connect()
        
    cursor = snowflake_handler.conn.cursor()
    
    #extract_load_opensky_data(columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, connection)
    
    aerodatabox_api_key_path = "credentials/aerodatabox_api_key.json"
    AERODATABOX_BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"
    endpoint = "flights/airports/"
    
    airports_query = "SELECT DISTINCT icao FROM airports"
    cursor.execute(airports_query)
    
    # Fetch all rows (each row is a tuple)
    rows = cursor.fetchall()

    # Extract only the airport values into a list
    airports_to_fetch = [row[0] for row in rows]

    if len(airports_to_fetch) > 0:
        logger.info(f"Fetched all airports' icao codes. \n airport: {airports_to_fetch}")
        extract_load_aerodatabox_data(
            aerodatabox_api_key_path,
            AERODATABOX_BASE_URL,
            endpoint,
            airports_to_fetch,
            date,
            connection
        )
    else:
        logger.error("No airports were fetched from snowflake.")
        raise Exception ("noAirportsData")
        
    extract_load_aerodatabox_data(aerodatabox_api_key_path, AERODATABOX_BASE_URL, endpoint, airports_to_fetch, date, snowflake_handler.conn)

if __name__ == "__main__":
    main()