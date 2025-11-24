import sys, os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import fetch_opensky_flight_data
from common_scripts.aerodatabox_connection import fetch_aerodatabox_data

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
                        ingestion_timestamp TIMESTAMP
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

def extract_load_aerodatabox_data(aerodatabox_api_key_path, BASE_URL, endpoint, airports_icao, date, cursor):
    
    logging.info("Started AeroDataBox arrivals and departures retrieval and loading process.............")
    
    _, _, departures, arrivals = fetch_aerodatabox_data(aerodatabox_api_key_path, BASE_URL, endpoint, airports_icao, date)
    
    if len(departures) > 0:
        
        table_name = 'airport_departures'
        
        logging.info(f"Creating table: {table_name} or checkting its existence.....")
        
        cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
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
                            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(2),
                            data_source VARCHAR(50) DEFAULT 'AeroDataBox',

                            departure_scheduledtime_utc TIMESTAMP,
                            departure_scheduledtime_local TIMESTAMP,
                            departure_revisedtime_utc TIMESTAMP,
                            departure_revisedtime_local TIMESTAMP,
                            departure_runwaytime_utc TIMESTAMP,
                            departure_runwaytime_local TIMESTAMP,
                            departure_terminal VARCHAR(10),
                            departure_runway VARCHAR(10),
                            
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
                            arrival_baggagebelt VARCHAR(20)
                            
                        )
                    """    
                    )
        
        logging.info(f"Created {table_name} table or existed")
        
        INSERT_DEPARTURES_QUERY = f"""
                    INSERT INTO {table_name} (
                        flight_number, flight_date, callsign, status, iscargo,
                        aircraft_reg, aircraft_modeS, aircraft_model,
                        airline_name, airline_iata, airline_icao, airport_icao,

                        departure_scheduledtime_utc, departure_scheduledtime_local,
                        departure_revisedtime_utc, departure_revisedtime_local,
                        departure_runwaytime_utc, departure_runwaytime_local,
                        departure_terminal, departure_runway, 

                        arrival_airport_icao, arrival_airport_iata, arrival_airport_name, arrival_airport_timezone,
                        arrival_scheduledtime_utc, arrival_scheduledtime_local,
                        arrival_revisedtime_utc, arrival_revisedtime_local,
                        arrival_runwaytime_utc, arrival_runwaytime_local,
                        arrival_terminal, arrival_gate, arrival_baggagebelt
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,

                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,

                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s
                    )
                """
        logging.info(f"Loading departures data into {table_name}.....")
                
        cursor.executemany(INSERT_DEPARTURES_QUERY, departures)
    
        logging.info("Finished departures data ingestion process finished.")
    else:
        logging.warning(f"Skipping loading, because departures data is empty .")
        pass

    if len(arrivals) > 0:
        table_name = 'airport_arrivals'
        
        logging.info(f"Creating table: {table_name} or checking its existence.....")
        
        cursor.execute(f"""
                   CREATE TABLE IF NOT EXISTS {table_name} (
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
                        
                        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(2),
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
                        
                        arrival_scheduledtime_utc TIMESTAMP,
                        arrival_scheduledtime_local TIMESTAMP,
                        arrival_revisedtime_utc TIMESTAMP,
                        arrival_revisedtime_local TIMESTAMP,
                        arrival_runwaytime_utc TIMESTAMP,
                        arrival_runwaytime_local TIMESTAMP,
                        arrival_terminal VARCHAR(10),
                        arrival_runway VARCHAR(10),
                        arrival_gate VARCHAR(10),
                        arrival_baggagebelt VARCHAR(20)
                    )
                   """)
        
        logging.info(f"Created {table_name} table or existed")
        
        INSERT_ARRIVALS_QUERY = f"""
                    INSERT INTO {table_name} (
                        flight_number, flight_date, callsign, status, iscargo,
                        aircraft_reg, aircraft_modeS, aircraft_model,
                        airline_name, airline_iata, airline_icao, airport_icao,

                        departure_airport_icao, departure_airport_iata, departure_airport_name, departure_airport_timezone,
                        departure_scheduledtime_utc, departure_scheduledtime_local,
                        departure_revisedtime_utc, departure_revisedtime_local,
                        departure_runwaytime_utc, departure_runwaytime_local,
                        departure_terminal, departure_runway,
                        arrival_scheduledtime_utc, arrival_scheduledtime_local,
                        arrival_revisedtime_utc, arrival_revisedtime_local,
                        arrival_runwaytime_utc, arrival_runwaytime_local,
                        arrival_terminal, arrival_runway, arrival_gate, arrival_baggagebelt
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,

                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
                    )
                    """
        logging.info(f"Loading arrivals data into {table_name}.....")
                
        cursor.executemany(INSERT_ARRIVALS_QUERY, arrivals)
    
        logging.info("Finished arrivals data ingestion process finished.")
    else:
        logging.warning(f"Skipping loading, because arrivals data is empty .")
        pass
    
    logging.info("Completed ingesting and loading both arrivals and departures data.")