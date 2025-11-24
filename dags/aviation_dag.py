from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

from src.flights_ingestion import extract_load_opensky_data
from src.arr_dep_ingestion import extract_load_aerodatabox_data
from snowflake_handler import SnowflakeHandler
from utils.logging import setup_logger

# Initialize paths
dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

# Environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dbt_project/profiles.yml'

# Define profile paths for the dbt_capstone project file
profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

schema = os.environ.get('SCHEMA')

def get_snowflake_connection(logger):
    """Reusable Snowflake connection initializer."""
    handler = SnowflakeHandler()

    if not handler.conn:
        logger.info("Connecting to Snowflake...")
        handler.connect()

    return handler.conn

@dag(
    dag_id="load_aviation_data",
    start_date=datetime(2025, 11, 23),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    default_args={
        "owner": schema,
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    }
)
def aviation_platform():

    # -----------------------------------------------------
    # Opensky Task
    # -----------------------------------------------------
    @task(task_id="opensky_flights")
    def opensky_flights_data(**context):
        logger = setup_logger("opensky_ingestion.log")

        execution_date = context["ds"]  # YYYY-MM-DD string

        columns = [
            'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen',
            'estArrivalAirport', 'callsign', 'estDepartureAirportHorizDistance',
            'estDepartureAirportVertDistance', 'estArrivalAirportHorizDistance',
            'estArrivalAirportVertDistance', 'departureAirportCandidatesCount',
            'arrivalAirportCandidatesCount', 'record_date'
        ]

        opensky_credentials = "credentials/opensky_credentials.json"
        OPENSKY_API = "https://opensky-network.org/api"
        endpoint = "/flights/all"
        table_name = "flights"

        connection = get_snowflake_connection(logger)

        extract_load_opensky_data(
            columns,
            opensky_credentials,
            OPENSKY_API,
            execution_date,
            endpoint,
            table_name,
            connection
        )

    # -----------------------------------------------------
    # AeroDataBox Task
    # -----------------------------------------------------
    @task(task_id="aerodatabox_dep_arr")
    def aerodatabox_dep_arr_data(**context):
        logger = setup_logger("aerodatabox_ingestion.log")

        execution_date = context["ds"]

        aerodatabox_key_path = "credentials/aerodatabox_api_key.json"
        AERODATABOX_API = "https://prod.api.market/api/v1/aedbx/aerodatabox"
        endpoint = "flights/airports/"

        airports_to_fetch = ["EDDF", "KJFK"]

        connection = get_snowflake_connection(logger)

        extract_load_aerodatabox_data(
            aerodatabox_key_path,
            AERODATABOX_API,
            endpoint,
            airports_to_fetch,
            execution_date,
            connection
        )

    # Task Dependencies
    opensky_flights_data() >> aerodatabox_dep_arr_data()

aviation_platform()