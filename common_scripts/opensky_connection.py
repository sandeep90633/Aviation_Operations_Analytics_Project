import requests
import logging
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader
from common_scripts.date_ranges import date_string_to_day_range_epoch

AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

def get_access_token(file_path):
    """Requests a new access token from the OpenSky auth server."""
    
    logging.info("Loading OpenSky Network credentials....")
    credentials = json_reader(file_path)
    
    # OpenSky API Client Credentials
    CLIENT_ID = credentials['clientId']
    CLIENT_SECRET = credentials['clientSecret']
    
    logging.info("Requesting new Access Token...")
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    
    try:
        response = requests.post(AUTH_URL, headers=headers, data=data)
        response.raise_for_status()  
        token_data = response.json()
        
        # The token is valid for 'expires_in' seconds (usually 1800 seconds or 30 minutes)
        access_token = token_data.get("access_token")
        
        logging.info(f"Successfully retrieved OpenSky Network Access Token.")
        return access_token
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error requesting token: {e}")
        raise e
    
def make_OpenSky_request(API_BASE_URL, endpoint, date, token):
    """Makes an API request using the Bearer Token."""
    if not token:
        logging.error("Error: No valid token available.")
        raise "notValidTokenError"
    
    url = f"{API_BASE_URL}{endpoint}"
    
    begin_ts, end_ts, _, _, _ = date_string_to_day_range_epoch(date)
    
    params = {
        "begin": begin_ts,
        "end": end_ts
    }
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    logging.info(f"params: {params}")
    logging.info(f"Making API request to {url}...")
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        
        remaining_credits = response.headers.get('X-Rate-Limit-Remaining')
        if remaining_credits is not None:
            logging.info(f"API Request Successful. Remaining Credits: {remaining_credits}")
        else:
            logging.warning("API Request Successful, but X-Rate-Limit-Remaining header was not found.")
            
        return response
    
    except requests.exceptions.HTTPError as e:
        # Check if the error is specifically a 404 (Not Found) and a 429 (request limit hit)
        if '404 Client Error' in str(e):
            logging.error(f"Resource not found (404) for URL: {response.url}.")
            raise ConnectionError (e)
        
        elif '429 Client Error' in str(e):
            
            logging.error(f"429 error received. Reached request limit. Stopping the script.")
            raise Exception ("Reached Request limit!")
        
        else:
            # Re-raise all other HTTP errors (400, 401, 500, etc.)
            logging.error(f"Critical HTTP Error: {e}")
            raise e   
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making API request: {e}")
        raise e
    
def fetch_opensky_flight_data(columns, opensky_cred_file, api_base_url, endpoint, date):
    """Fetches flight data for multiple airports with retry and token refresh logic."""
    
    # Initialize variables
    token = get_access_token(opensky_cred_file)
    all_records = []
    MAX_RETRIES = 2

    retry = 0
    while retry < MAX_RETRIES:
        
        response = make_OpenSky_request(api_base_url, endpoint, date, token)
        try:
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Successfully retrieved opensky records on date: {date}.")
                logging.info("Parsing the results.....")
                records = [tuple(item.get(col) for col in columns[0:-1]) + (date,) for item in data]
                
                all_records.extend(records)
                
                logging.info("Results were added to global records variable.")
                retry = MAX_RETRIES # Success, break out of while loop

            elif response.status_code == 401:
                logging.warning("Token might have expired. Requesting new token...")
                token = get_access_token(opensky_cred_file)
                
                retry += 1
                
                if retry == MAX_RETRIES:
                    logging.error(f"Failed to refresh token after {MAX_RETRIES} attempts.")
                    raise ConnectionError ("Token Error.")
                
            else:
                logging.error(f"Status Code: {response.status_code}. Response: {response.text}")
                # log the error and break the while loop to continue with other icaos
                raise Exception (f"Status Code: {response.status_code}. Response: {response.text}")
            
        except:
            logging.error("Failed while parsing the response.")
            raise Exception ("Failed while parsing the response.")

    return all_records, columns