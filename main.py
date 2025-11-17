from src.ingestion import extract_load_opensky_data
from utils.logging import setup_logger
from snowflake_handler import SnowflakeHandler
import logging

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
    
    date = "2025-01-01"
    
    logger.info("Started OpenSky Network arrival and departure retrieval and loading process.............")
    
    snowflake_handler = SnowflakeHandler(
        credentials_dir="credentials", 
        file_name="snowflake_configs.json"
    )

    if not snowflake_handler.conn:
        logger.info("Connecting to Snowflake...")
        snowflake_handler.connect()
        
    
    extract_load_opensky_data(columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, snowflake_handler.conn)

if __name__ == "__main__":
    main()