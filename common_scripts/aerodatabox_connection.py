import requests
import urllib.parse
import sys, os
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader
from common_scripts.date_ranges import date_string_to_day_range_epoch
class AeroDataBoxAPIError(Exception):
    """Custom exception for AeroDataBox API errors."""
    pass

def make_aerodatabox_request(api_key: str, base_url: str, endpoint: str, code_type: str,
                             code: str, time_from: str, time_to: str, timeout: int = 60):
    """
    Makes a request to the AeroDataBox API for flight arrivals/departures.

    Args:
        api_key (str): API key string.
        base_url (str): Base URL of the AeroDataBox API.
        endpoint (str): API endpoint (e.g., 'flights').
        code_type (str): Type of code ('icao' or 'iata').
        code (str): Airport code.
        time_from (str): Start time (ISO8601 or epoch string).
        time_to (str): End time (ISO8601 or epoch string).
        timeout (int): Request timeout in seconds. Default is 15.

    Returns:
        requests.Response: The successful response object.

    Raises:
        AeroDataBoxAPIError: If any error occurs during the request or response validation.
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
            "flight_number": get_value(record, "number"),
            "flight_date": date,
            "callsign": get_value(record, "callSign"),
            "status": get_value(record, "codeshareStatus"),
            "iscargo": get_value(record, "isCargo"),
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
    
    credentials = json_reader(api_key_file_path)
    api_key = credentials['key']
    
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