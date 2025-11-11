import json
import logging

def json_reader(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
            logging.info("Successfully loaded credentials.")
        return credentials
    except FileNotFoundError:
        logging.error(f"Error: The file '{file_path}' was not found. Please check the path.")
    except json.JSONDecodeError:
        logging.error(f"Error: The file '{file_path}' is not valid JSON.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")