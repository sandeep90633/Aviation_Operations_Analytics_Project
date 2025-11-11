import requests
import urllib.parse
import sys, os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader

def make_aerodatabox_request(api_key_file_path, BASE_URL, code_type, code, TIME_FROM, TIME_TO):
    
    credentials = json_reader(api_key_file_path)

    # 1. URL-encode the time strings
    encoded_from = urllib.parse.quote(TIME_FROM)
    encoded_to = urllib.parse.quote(TIME_TO)

    headers = {
        "accept": "application/json",
        "x-api-market-key": credentials['key'],
    }

    # Combine the base URL and the endpoint for the final request URL
    full_url = f"{BASE_URL}/{code_type}/{code}/{encoded_from}/{encoded_to}"

    try:
        # Use the full_url for the request
        response = requests.get(full_url, headers=headers)
        
        # Check for HTTP errors before trying to parse JSON
        response.raise_for_status() 
        
        print(response.json())
        
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as e:
        # This catches the original exception and others not caught above
        logging.error(f"An unexpected API request error occurred: {e}")
    except Exception as e:
        # Catches non-request errors, like JSON decoding failure
        logging.error(f"An unexpected error occurred: {e}")