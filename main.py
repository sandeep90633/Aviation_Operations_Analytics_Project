from src.ingestion import extract_load_opensky_data
from utils.logging import setup_logger
from snowflake_handler import SnowflakeHandler

logger = setup_logger('opensky_ingestion.log')

def main():
    
    columns = [
        'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen',
        'estArrivalAirport', 'callsign', 'estDepartureAirportHorizDistance',
        'estDepartureAirportVertDistance', 'estArrivalAirportHorizDistance',
        'estArrivalAirportVertDistance', 'departureAirportCandidatesCount',
        'arrivalAirportCandidatesCount', 'airport_icao', 'record_date'
    ]
    
    opensky_cred_file = "credentials/opensky_credentials.json"
    
    OPENSKY_API_BASE_URL = "https://opensky-network.org/api"
    
    date = "2022-01-01"
    
    snowflake_handler = SnowflakeHandler(
        credentials_dir="credentials", 
        file_name="snowflake_configs.json"
    )

    if not snowflake_handler.conn:
        logger.info("Connecting to Snowflake...")
        snowflake_handler.connect()
        
    airports_icao_query = "SELECT DISTINCT icao FROM airports"
    
    cursor = snowflake_handler.conn.cursor()
    
    cursor.execute(airports_icao_query)

    # Fetch all rows
    rows = cursor.fetchall()

    # Extract the ICAO codes into a simple list
    airports_icao = [row[0] for row in rows]
    
    extract_load_opensky_data(airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, snowflake_handler.conn)

if __name__ == "__main__":
    main()