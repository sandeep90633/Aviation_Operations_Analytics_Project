import sys, os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import fetch_opensky_flight_data
from common_scripts.aerodatabox_connection import fetch_arrivals_departures_data

def extract_load_opensky_data(airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, date, cursor):
    
    logging.info("Started OpenSky Network arrival and departure retrieval and loading process.............")
    
    # Two different directions for each airport
    directions = ['departure', 'arrival']
    
    for direction in directions:
        logging.info(f"Started retreival process for {direction}...")
        data, columns = fetch_opensky_flight_data(airports_icao, columns, opensky_cred_file, OPENSKY_API_BASE_URL, f"/flights/{direction}", date)
        
        if len(data) > 0:
            logging.info(f"'{direction}' data retreival completed, preparing table creation or checking....")
            
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
                
            logging.info(f"Table flights_{direction}s is created or existed.")
            
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
            logging.info(f"Inserting data into snowflake table 'flight_{direction}s'...")
            cursor.executemany(insert_sql, data)
            logging.info(f"'{direction}' data ingestion process finished.")  
        else:
            logging.warning(f"Skipping loading, because '{direction}' data is empty .")
            pass