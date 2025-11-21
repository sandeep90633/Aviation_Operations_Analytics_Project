from datetime import datetime
from astro.providers.dbt.task_group import DbtTaskGroup
from airflow import DAG

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    dbt_tasks = DbtTaskGroup(
        project_name="my_dbt_project",
        conn_id=None,   # not needed if using env vars
        profile_args={"target": "dev"},
        default_args={"dbt_root_path": "/usr/local/airflow/dbt"},
    )

    dbt_tasks