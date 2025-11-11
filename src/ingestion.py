import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common_scripts.opensky_connection import make_OpenSky_request
from logging import setup_logger