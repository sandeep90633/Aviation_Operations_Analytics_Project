import datetime
import logging
from typing import Optional, Tuple

def date_string_to_day_range_epoch(date_string: str, date_format: str = "%Y-%m-%d") -> Optional[Tuple[int, int, str, str, str]]:
    """
    Takes a date string, calculates the start, midday, and end of that day in UTC,
    and returns both as epoch timestamps AND formatted strings for APIs.
    
    The function performs the following:
    1. Parses the date_string to find midnight (00:00:00) UTC for that day (Start).
    2. Calculates midday (12:00:00) UTC for that day (Midday String).
    3. Calculates midnight (23:59:59) UTC for that day (End Exclusive Epoch).
    4. Calculates 23:59 for the current day (End Inclusive String).

    Args:
        date_string: The input date string (e.g., "2025-11-02").
        date_format: The format of the input string (default: YYYY-MM-DD).

    Returns:
        A tuple: (
            start_epoch, 
            end_epoch_exclusive, 
            start_str,
            midday_str, 
            end_str_inclusive
        ) 
        as integers and strings, or None if conversion fails.
    """
    if not date_string:
        logging.error("Input date string is empty or None.")
        raise ValueError("A date string must be provided for processing.")
        
    try:
        # Parse the string into a datetime object, setting to midnight UTC
        dt_start_of_day = datetime.datetime.strptime(date_string, date_format).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        )

        # Calculate the exclusive end of day (midnight of the next day)
        dt_end_of_day_exclusive_boundary = dt_start_of_day + datetime.timedelta(days=1)
        
        # Calculate the inclusive end of day (23:59 of the current day)
        dt_end_of_day_inclusive = dt_end_of_day_exclusive_boundary - datetime.timedelta(minutes=1)
        
        # 4. Calculate midday (12:00:00 of the current day)
        dt_midday = dt_start_of_day + datetime.timedelta(hours=12)

        # Convert both boundaries to epoch
        start_epoch = int(dt_start_of_day.timestamp())
        end_epoch = int(dt_end_of_day_exclusive_boundary.timestamp()) - 1
        
        # Produce required strings (e.g., "2025-11-02T00:00")
        API_TIME_FORMAT = "%Y-%m-%dT%H:%M"
        start_str = dt_start_of_day.strftime(API_TIME_FORMAT)
        midday_str = dt_midday.strftime(API_TIME_FORMAT)
        end_str = dt_end_of_day_inclusive.strftime(API_TIME_FORMAT)

        logging.debug(
            f"Date string '{date_string}' converted to range: Epoch=[{start_epoch}, {end_epoch}); Strings=[{start_str}, {end_str}]"
        )
        return start_epoch, end_epoch, start_str, midday_str, end_str

    except ValueError as e:
        logging.error(f"Date conversion failed for string '{date_string}' with format '{date_format}': {e}")
        return ValueError
    except Exception as e:
        logging.error(f"An unexpected error occurred during day range conversion: {e}")
        return Exception