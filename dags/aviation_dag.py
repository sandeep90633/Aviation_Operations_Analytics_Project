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

logger = setup_logger('aviation_operations.log')

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

    connection = handler.conn
    cursor = handler.conn.cursor()
    
    return connection, cursor

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

        connection, _ = get_snowflake_connection(logger)

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

        execution_date = context["ds"]

        aerodatabox_key_path = "credentials/aerodatabox_api_key.json"
        AERODATABOX_API = "https://prod.api.market/api/v1/aedbx/aerodatabox"
        endpoint = "flights/airports/"

        connection, cursor = get_snowflake_connection(logger)
        
        logger.info("Executing query to fetch the airports.....")
        airports_query = "SELECT icao FROM airports WHERE COUNTRY IN ('DE', 'FR', 'CH','AE')"
        cursor.execute(airports_query)
        
        # Fetch all rows (each row is a tuple)
        rows = cursor.fetchall()
    
        # Extract only the airport values into a list
        airports_to_fetch = [row[0] for row in rows]
        
        if len(airports_to_fetch) > 0:
            logger.info(f"Fetched all airports' icao codes. \n airport: {airports_to_fetch}")
            extract_load_aerodatabox_data(
                aerodatabox_key_path,
                AERODATABOX_API,
                endpoint,
                airports_to_fetch,
                execution_date,
                connection
            )
        else:
            logger.error("No airports were fetched from snowflake.")
            raise Exception ("noAirportsData")
    
    dbt_stg_airports = DbtTaskGroup(
        group_id="airports_data",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["+stg_airports"],
        ),
    )
        
    dbt_stg_flights = DbtTaskGroup(
        group_id="flights_data",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["+stg_flights"],
        ),
    )
    
    dbt_stg_departures = DbtTaskGroup(
        group_id="departures_data",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["+stg_airport_departures"],
        ),
    )
    
    dbt_stg_arrivals = DbtTaskGroup(
        group_id="arrivals_data",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["+stg_airport_arrivals"],
        ),
    )

    # Task Dependencies
    [opensky_flights_data(), aerodatabox_dep_arr_data()] >> dbt_stg_airports >> dbt_stg_flights >> dbt_stg_departures >> dbt_stg_arrivals 

aviation_platform()