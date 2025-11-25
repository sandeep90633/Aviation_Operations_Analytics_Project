import sys, os
import logging
import requests
import urllib.parse
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader
from utils.date_ranges import date_string_to_day_range_epoch
from utils.transaction_cursor import transaction

load_dotenv()
class AeroDataBoxAPIError(Exception):
    """Custom exception for AeroDataBox API errors."""
    pass

def make_aerodatabox_request(api_key: str, base_url: str, endpoint: str, code_type: str,
                             code: str, time_from: str, time_to: str, timeout: int = 60):
    """
    Makes a request to the AeroDataBox API for flight arrivals/departures.

    """
    encoded_from = urllib.parse.quote(time_from)
    encoded_to = urllib.parse.quote(time_to)

    headers = {
        "accept": "application/json",
        "x-api-market-key": api_key,
    }

    params = {"withLeg": True}

    full_url = f"{base_url}/{endpoint}/{code_type}/{code}/{encoded_from}/{encoded_to}"

    logging.info(f"Sending API request for ICAO: {code}")
    logging.debug(f"Full URL: {full_url}")

    try:
        response = requests.get(full_url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx
        return response

    except requests.exceptions.HTTPError as errh:
        msg = f"HTTP error for {code}: {errh.response.status_code} - {errh.response.text}"
        logging.error(msg)
        raise AeroDataBoxAPIError(msg) from errh

    except requests.exceptions.ConnectionError as errc:
        msg = f"Connection error while reaching AeroDataBox for {code}: {errc}"
        logging.error(msg)
        raise AeroDataBoxAPIError(msg) from errc

    except requests.exceptions.Timeout as errt:
        msg = f"Request timeout for {code}: {errt}"
        logging.error(msg)
        raise AeroDataBoxAPIError(msg) from errt

    except requests.exceptions.RequestException as err:
        msg = f"Unexpected request error for {code}: {err}"
        logging.error(msg)
        raise AeroDataBoxAPIError(msg) from err

    except Exception as e:
        msg = f"Unhandled exception for {code}: {e}"
        logging.exception(msg)  # Includes traceback
        raise AeroDataBoxAPIError(msg) from e
        
def get_value(data, path, default=None):
    """Safely get a nested value from a dict using dot notation."""
    keys = path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data

def fetch_aerodatabox_data(api_key_file_path: str, base_url: str, endpoint: str, airports_icao: list[str], date: str):
    """
    Fetch arrivals and departures data from AeroDataBox for a given airport and date.
    Returns tuples of (departures, arrivals) with column order preserved.
    """

    # --- Helper functions ---

    def base_flight_fields(record: dict, airport_icao: str) -> dict:
        """Fields shared by both arrivals and departures."""
        return {
            "number": get_value(record, "number"),
            "flight_date": date,
            "callSign": get_value(record, "callSign"),
            "status": get_value(record, "status"),
            "codeshareStatus": get_value(record, "codeshareStatus"),
            "isCargo": get_value(record, "isCargo"),
            "aircraft_reg": get_value(record, "aircraft.reg"),
            "aircraft_modeS": get_value(record, "aircraft.modeS"),
            "aircraft_model": get_value(record, "aircraft.model"),
            "airline_name": get_value(record, "airline.name"),
            "airline_iata": get_value(record, "airline.iata"),
            "airline_icao": get_value(record, "airline.icao"),
            "airport_icao": airport_icao
            #"ingestion_timestamp": None,   Will be defualt while table creation
            #"data_source": "AeroDataBox"  Will be defualt while table creation
        }

    def parse_departure_record(dep: dict, airport_icao: str) -> dict:
        rec = base_flight_fields(dep, airport_icao)
        rec.update({
            # Current airport = departure
            "departure_scheduledtime_utc": get_value(dep, "departure.scheduledTime.utc"),
            "departure_scheduledtime_local": get_value(dep, "departure.scheduledTime.local"),
            "departure_revisedtime_utc": get_value(dep, "departure.revisedTime.utc"),
            "departure_revisedtime_local": get_value(dep, "departure.revisedTime.local"),
            "departure_runwaytime_utc": get_value(dep, "departure.runwayTime.utc"),
            "departure_runwaytime_local": get_value(dep, "departure.runwayTime.local"),
            "departure_terminal": get_value(dep, "departure.terminal"),
            "departure_runway": get_value(dep, "departure.runway"),
            #"departure_quality": get_value(dep, "departure.quality"), Having problem with the list while uploading
            # Destination airport info
            "arrival_airport_icao": get_value(dep, "arrival.airport.icao"),
            "arrival_airport_iata": get_value(dep, "arrival.airport.iata"),
            "arrival_airport_name": get_value(dep, "arrival.airport.name"),
            "arrival_airport_timezone": get_value(dep, "arrival.airport.timeZone"),
            "arrival_scheduledtime_utc": get_value(dep, "arrival.scheduledTime.utc"),
            "arrival_scheduledtime_local": get_value(dep, "arrival.scheduledTime.local"),
            "arrival_revisedtime_utc": get_value(dep, "arrival.revisedTime.utc"),
            "arrival_revisedtime_local": get_value(dep, "arrival.revisedTime.local"),
            "arrival_runwaytime_utc": get_value(dep, "arrival.runwayTime.utc"),
            "arrival_runwaytime_local": get_value(dep, "arrival.runwayTime.local"),
            "arrival_terminal": get_value(dep, "arrival.terminal"),
            "arrival_gate": get_value(dep, "arrival.gate"),
            "arrival_baggagebelt": get_value(dep, "arrival.baggageBelt"),
            #"arrival_quality": get_value(dep, "arrival.quality")
        })

        return rec

    def parse_arrival_record(arr: dict, airport_icao: str) -> dict:
        rec = base_flight_fields(arr, airport_icao)
        rec.update({
            # Origin airport info (note: fixed path naming bug)
            "departure_airport_icao": get_value(arr, "departure.airport.icao"),
            "departure_airport_iata": get_value(arr, "departure.airport.iata"),
            "departure_airport_name": get_value(arr, "departure.airport.name"),
            "departure_airport_timezone": get_value(arr, "departure.airport.timeZone"),
            "departure_scheduledtime_utc": get_value(arr, "departure.scheduledTime.utc"),
            "departure_scheduledtime_local": get_value(arr, "departure.scheduledTime.local"),
            "departure_revisedtime_utc": get_value(arr, "departure.revisedTime.utc"),
            "departure_revisedtime_local": get_value(arr, "departure.revisedTime.local"),
            "departure_runwaytime_utc": get_value(arr, "departure.runwayTime.utc"),
            "departure_runwaytime_local": get_value(arr, "departure.runwayTime.local"),
            "departure_terminal": get_value(arr, "departure.terminal"),
            "departure_runway": get_value(arr, "departure.runway"),
            #"departure_quality": get_value(arr, "departure.quality"),
            # Current airport = arrival
            "arrival_scheduledtime_utc": get_value(arr, "arrival.scheduledTime.utc"),
            "arrival_scheduledtime_local": get_value(arr, "arrival.scheduledTime.local"),
            "arrival_revisedtime_utc": get_value(arr, "arrival.revisedTime.utc"),
            "arrival_revisedtime_local": get_value(arr, "arrival.revisedTime.local"),
            "arrival_runwaytime_utc": get_value(arr, "arrival.runwayTime.utc"),
            "arrival_runwaytime_local": get_value(arr, "arrival.runwayTime.local"),
            "arrival_terminal": get_value(arr, "arrival.terminal"),
            "arrival_runway": get_value(arr, "arrival.runway"),
            "arrival_gate": get_value(arr, "arrival.gate"),
            "arrival_baggagebelt": get_value(arr, "arrival.baggageBelt"),
            #"arrival_quality": get_value(arr, "arrival.quality")
        })
        return rec
    
    aerodatabox_api_key = os.getenv('AERODATABOX_API_KEY')
    
    if not aerodatabox_api_key:
        logging.info("AeroDataBox api key was not provided via .env, taking it from local json file.")
        credentials = json_reader(api_key_file_path)
    
    api_key = aerodatabox_api_key or credentials['key']
    
    _, _, start_str, mid_str, end_str = date_string_to_day_range_epoch(date)
    
    # --- API calls in two halves ---
    halves = [
        ("first_half", start_str, mid_str),
        ("second_half", mid_str, end_str)
    ]

    departure_records, arrival_records = [], []

    for half_name, time_from, time_to in halves:
        
        for airport_icao in airports_icao:
            response = make_aerodatabox_request(api_key, base_url, endpoint, "icao", airport_icao, time_from, time_to)
            
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Retrieved flight data for {airport_icao} ({half_name}).")

                departures = data.get("departures", [])
                arrivals = data.get("arrivals", [])

                departure_records.extend(parse_departure_record(d, airport_icao) for d in departures)
                arrival_records.extend(parse_arrival_record(a, airport_icao) for a in arrivals)

                if not departures:
                    logging.warning(f"No departures found for {airport_icao} ({half_name}).")
                if not arrivals:
                    logging.warning(f"No arrivals found for {airport_icao} ({half_name}).")

            elif response.status_code == 204:
                logging.warning(f"No content for {airport_icao} in {half_name}.")
                continue
            else:
                logging.error(f"AeroDataBox API error {response.status_code}: {response.text}")
                raise RuntimeError(f"AeroDataBox API error {response.status_code}: {response.text}")

    # Safely derive schema
    departure_columns = list(departure_records[0].keys()) if departure_records else []
    arrival_columns = list(arrival_records[0].keys()) if arrival_records else []

    all_departures = [tuple(rec.get(col) for col in departure_columns) for rec in departure_records]
    all_arrivals = [tuple(rec.get(col) for col in arrival_columns) for rec in arrival_records]
    
    logging.info("All departures and arrivals data are ready for the given airports.")

    return arrival_columns, departure_columns, all_departures, all_arrivals
    
def _ingest_aerodatabox_data(cursor, data, table_name, column_names, specific_cols_sql):
    """Handles table creation and data insertion for a single AeroDataBox dataset."""
    if not data:
        logging.warning(f"Skipping loading, because {table_name} data is empty.")
        return

    # Base columns common to both AeroDataBox tables
    base_columns_sql = """
        number VARCHAR(20), flight_date DATE NOT NULL, callSign VARCHAR(20),
        status VARCHAR(50), codeshareStatus VARCHAR(50), isCargo BOOLEAN, aircraft_reg VARCHAR(20),
        aircraft_modeS VARCHAR(20), aircraft_model VARCHAR(100), airline_name VARCHAR(100),
        airline_iata VARCHAR(10), airline_icao VARCHAR(10), airport_icao VARCHAR(10) NOT NULL,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
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
    
    logging.info(f"Started AeroDataBox arrivals and departures retrieval and loading process for the date : {date}.............")
    
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
        'number', 'flight_date', 'callSign', 'status', 'codeshareStatus', 'isCargo', 'aircraft_reg', 'aircraft_modeS', 'aircraft_model',
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
        'number', 'flight_date', 'callSign', 'status', 'codeshareStatus', 'isCargo', 'aircraft_reg', 'aircraft_modeS', 'aircraft_model',
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