from src.ingestion import extract_load_opensky_data
from utils.logging import setup_logger
from snowflake_handler import SnowflakeHandler

logger = setup_logger('opensky_ingestion.log')

def main():
    
    airports_icao = ['EDDN', 'EDDF', 'EDDM', 'KJFK']
    columns = [
        'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen',
        'estArrivalAirport', 'callsign', 'estDepartureAirportHorizDistance',
        'estDepartureAirportVertDistance', 'estArrivalAirportHorizDistance',
        'estArrivalAirportVertDistance', 'departureAirportCandidatesCount',
        'arrivalAirportCandidatesCount', 'airport_icao', 'record_date'
    ]
    
    opensky_cred_file = "credentials/opensky_credentials.json"
    
    OPENSKY_API_BASE_URL = "https://opensky-network.org/api"
    
    date = "2025-01-03"
    
    snowflake_handler = SnowflakeHandler(
        credentials_dir="credentials", 
        file_name="snowflake_configs.json"
    )

    if not snowflake_handler.conn:
        logger.info("Connecting to Snowflake...")
        snowflake_handler.connect()
    
    extract_load_opensky_data(airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, snowflake_handler.conn)

if __name__ == "__main__":
    main()