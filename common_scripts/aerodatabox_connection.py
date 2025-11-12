import requests
import urllib.parse
import sys, os
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader
from common_scripts.date_ranges import date_string_to_day_range_epoch

def make_aerodatabox_request(api_key, BASE_URL, code_type, code, TIME_FROM, TIME_TO):
    
    # 1. URL-encode the time strings
    encoded_from = urllib.parse.quote(TIME_FROM)
    encoded_to = urllib.parse.quote(TIME_TO)

    headers = {
        "accept": "application/json",
        "x-api-market-key": api_key,
    }
    
    full_url = f"{BASE_URL}/{code_type}/{code}/{encoded_from}/{encoded_to}"

    logging.info(f"Sending api request for the icao: {code}.....")
    logging.info(f"URL: {full_url}")
    
    try:
        response = requests.get(full_url, headers=headers)
        # Check for HTTP errors before trying to parse JSON
        response.raise_for_status() 
        
        return response
        
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An unexpected API request error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        
def get_value(data, path, default=None):
    """Safely get a nested value from a dict using dot notation."""
    keys = path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data

def fetch_arrivals_departures_data(api_key_file_path, BASE_URL, airport_icao, date):
    
    credentials = json_reader(api_key_file_path)
    api_key = credentials['key']
    
    _, _, start_str, mid_str, end_str = date_string_to_day_range_epoch(date)
       
    departure_records=[]
    arrival_records=[]
    
    day = ['first_half', 'second_half']  # the maximum time that aerodatabox allows in 12hrs, so two times request with changing times
    
    for half in day:
        
        if half == 'first_half':
            TIME_FROM = start_str
            TIME_TO = mid_str
        else:
            TIME_FROM = mid_str
            TIME_TO = end_str
        
        response = make_aerodatabox_request(api_key, BASE_URL, "icao", airport_icao, TIME_FROM, TIME_TO)
        
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Successfully retrieved Aircraft vector records for {airport_icao}.")
            
            departures = data['departures']
            arrivals = data['arrivals']
        
            if len(departures) > 0:
                
                for departure in departures:
                    departure_record = {
                            "flight_number": get_value(departure, "number"),
                            "flight_date": date,
                            "callsign": get_value(departure, "callSign"),
                            "status": get_value(departure, "codeshareStatus"),
                            "iscargo": get_value(departure, "isCargo"),
                            "aircraft_reg": get_value(departure, "aircraft.reg"),
                            "aircraft_modeS": get_value(departure, "aircraft.modeS"),
                            "aircraft_model": get_value(departure, "aircraft.model"),
                            "airline_name": get_value(departure, "airline.name"),
                            "airline_iata": get_value(departure, "airline.iata"),
                            "airline_icao": get_value(departure, "airline.icao"),
                            "airport_icao": airport_icao,
                            "departure_scheduledtime_utc": get_value(departure, "departure.scheduledTime.utc"),
                            "departure_scheduledtime_local": get_value(departure, "departure.scheduledTime.local"),
                            "departure_revisedtime_utc": get_value(departure, "departure.revisedTime.utc"),
                            "departure_revisedtime_local": get_value(departure, "departure.revisedTime.local"),
                            "departure_runwaytime_utc": get_value(departure, "departure.runwayTime.utc"),
                            "departure_runwaytime_local": get_value(departure, "departure.runwayTime.local"),
                            "departure_terminal": get_value(departure, "departure.terminal"),
                            "departure_quality": get_value(departure, "departure.quality"),
                            "arrival_airport_icao": get_value(departure, "arrival.airport.icao"),
                            "arrival_airport_iata": get_value(departure, "arrival.airport.iata"),
                            "arrival_airport_name": get_value(departure, "arrival.airport.name"),
                            "arrival_airport_timezone": get_value(departure, "arrival.airport.timeZone"),
                            "arrival_scheduledtime_utc": get_value(departure, "arrival.scheduledTime.utc"),
                            "arrival_scheduledtime_local": get_value(departure, "arrival.scheduledTime.local"),
                            "arrival_revisedtime_utc": get_value(departure, "arrival.revisedTime.utc"),
                            "arrival_revisedtime_local": get_value(departure, "arrival.revisedTime.local"),
                            "arrival_runwaytime_utc": get_value(departure, "arrival.runwayTime.utc"),
                            "arrival_runwaytime_local": get_value(departure, "arrival.runwayTime.local"),
                            "arrival_terminal": get_value(departure, "arrival.terminal"),
                            "arrival_gate": get_value(departure, "arrival.gate"),
                            "arrival_baggagebelt": get_value(departure, "arrival.baggageBelt"),
                            "arrival_quality": get_value(departure, "arrival.quality"),
                            "ingestion_timestamp": datetime.utcnow().isoformat(),
                            "data_source": "AeroDataBox"
                        }
                    
                    departure_records.append(departure_record)
            else:
                logging.warn(f"No departures in the airport: {airport_icao} on date: {date}")
                
        elif response.status_code == 204:
            logging.warn(f"No content for the {airport_icao} in '{half}'.")
            break
            
        else:
            logging.error(f"Error for {airport_icao}. Status Code: {response.status_code}. Response: {response.text}")
            raise Exception
        
    
    departure_columns = list(departure_record.keys())
                
    all_departures = [tuple(item.get(col) for col in departure_columns) for item in departure_records]