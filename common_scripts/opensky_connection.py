import requests
import logging
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_reader import json_reader

AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

def get_access_token(file_path):
    """Requests a new access token from the OpenSky auth server."""
    
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
    
def make_OpenSky_request(API_BASE_URL, endpoint, param, airport_or_icao24_value, begin_ts, end_ts, token):
    """Makes an API request using the Bearer Token."""
    if not token:
        logging.error("Error: No valid token available.")
        raise "notValidTokenError"
        
    url = f"{API_BASE_URL}{endpoint}"
    logging.info(f"\nMaking API request to {url}...")
    
    # As this function works for two REST API endpoints, depending on the param provided the params were selected  
    if param == "airport":
        params = {
            "airport": airport_or_icao24_value,
            "begin": begin_ts,
            "end": end_ts
        }
    else:
        params = {
            "icao24": airport_or_icao24_value,
            "begin": begin_ts,
            "end": end_ts
        }
    
    logging.info(f"Param:'{param}' is selected, therefore params: {params}")
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        return response
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making API request: {e}")
        raise e