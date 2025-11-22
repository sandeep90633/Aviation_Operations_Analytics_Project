import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

airflow_home=os.environ['AIRFLOW_HOME']

# Define paths to the DBT project and virtual environment
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/dbt'

default_args = {
  "owner": "Sandeep",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2025, 11, 21),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)
def dbt_dag_bash_exploded():
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_stg_flights = BashOperator(
        task_id='dbt_stg_flights',
        bash_command=f'{PATH_TO_DBT_VENV} build -s stg_flights',
        cwd=PATH_TO_DBT_PROJECT,
    )

    # Post DBT workflow task
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    # Define task dependencies
    pre_dbt_workflow >> dbt_stg_flights >> post_dbt_workflow


dbt_dag_bash_exploded()