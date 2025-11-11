import csv
from snowflake_handler import SnowflakeHandler
from utils.logging import setup_logger

logger = setup_logger('airport_data_ingest.log')

def load_airports_data(file_path: str) -> list:
    """
    Reads a CSV file containing airport data and converts each row into a tuple.

    Args:
        file_path: The relative or absolute path to the airports CSV file.

    Returns:
        A list of tuples, where each tuple represents one airport record.
    """
    airports = []
    try:
        
        with open(file_path, 'r', encoding='utf-8') as f:
            # csv.reader returns an iterable of list of strings for each row
            data = csv.reader(f)
            
            try:
                # Retrieve and discard the first row (the header)
                next(data) 
            except StopIteration:
                logger.error(f"File {file_path} is empty after opening.")
                raise "FileEmptyError"
            
            for line in data:
                airports.append(tuple(line))
        
        logger.info(f"Successfully loaded {len(airports)} records from {file_path}")
        return airports

    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at path: {file_path}")
        return []
    except Exception as e:
        logger.critical(f"An unexpected error occurred during file reading: {e}")
        return []

def main():
    
    logger.info("Starting airport data ingestion process")
    
    airport_data = load_airports_data('airports.csv')
    
    if not airport_data:
        logger.warning("No airport data to process. Exiting main function.")
        return
    
    try:
        snowflake_handler = SnowflakeHandler(
            credentials_dir="credentials", 
            file_name="snowflake_configs.json"
        )

        if not snowflake_handler.conn:
            logger.info("Connecting to Snowflake...")
            snowflake_handler.connect()
        
        cursor = snowflake_handler.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airports (
                airport VARCHAR(100),
                country VARCHAR(10),
                state VARCHAR(20),
                city VARCHAR(100),
                icao VARCHAR(20),
                iata VARCHAR(20),
                elevation_ft FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                PRIMARY KEY (airport)
            )
            """)
        
        logger.info("Table airports is created or existed.")
        
        insert_sql = """
            INSERT INTO airports
            (airport, country, state, city, icao, iata, elevation_ft, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        cursor.executemany(insert_sql, airport_data)

        logger.info("Connection successful and data ready for insertion.")

    except Exception as e:
        logger.critical(f"Failed during Snowflake connection or handling: {e}")
        
    finally:
        
        if snowflake_handler.conn:
            snowflake_handler.close()
            logger.info("Snowflake connection closed.")
        
    #logger.info("Airport data ingestion process finished.")

if __name__ == "__main__":
    main()
    
