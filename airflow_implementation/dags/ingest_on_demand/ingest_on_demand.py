from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pathlib import Path
from helpers import DMLScripts
from operators import DataQualityOperator, LoadCSVFromDriveOperator
import os

from datetime import datetime, timedelta

POSTGRES_CONN_ID = "airflow_db"

default_args = {
    "owner": "erickramireztebalan@gmail.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag_name = "ingest_on_demand"

current_work_directory = Path(os.path.dirname(os.path.realpath(__file__)))
sql_folder = os.path.join(current_work_directory, "dag_dependencies")
PG_FINAL_SCHEMA = "trips"
PG_STAGE_SCHEMA = "trips_stage"

VARIABLE_DICT = {
    "PG_STAGE_SCHEMA": PG_STAGE_SCHEMA,
    "PG_FINAL_SCHEMA": PG_FINAL_SCHEMA,
}


def get_query(query_file_name: str, variable_dict: dict) -> str:
    """
        get query to execute
    :param query_file_name: file name
    :param variable_dict: dictionary to update the queries
    :return: query string
    """
    with open(os.path.join(sql_folder, query_file_name)) as f:
        query = f.read()
    query = query.format(**variable_dict)
    return query


create_stage_schema_and_tables = "create_stage_schema_and_tables.sql"
create_final_schema_and_tables = "create_final_schema_and_tables.sql"
with DAG(
    dag_name,
    start_date=datetime(2022, 3, 28),
    max_active_runs=3,
    schedule_interval="0 1 * * *",  # At 01:00.
    default_args=default_args,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")
    load_csv_task = LoadCSVFromDriveOperator(
        task_id="load-csv",
        dag=dag,
        conn_id=POSTGRES_CONN_ID,
        table_name=f"{PG_STAGE_SCHEMA}.trips",
        uri_csv_file="https://drive.google.com/file/d/14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU/view?usp=sharing",
    )
    create_stage_schema_and_tables_task = PostgresOperator(
        task_id="create-stage-schemas-and-tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=get_query(create_stage_schema_and_tables, VARIABLE_DICT),
    )
    create_final_schema_and_tables_task = PostgresOperator(
        task_id="create-final-schemas-and-tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=get_query(create_final_schema_and_tables, VARIABLE_DICT),
    )

    fill_dim_datasource_task = PostgresOperator(
        task_id="fill-dim-datasource",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.datasource_table_insert.format(**VARIABLE_DICT),
    )

    fill_region_task = PostgresOperator(
        task_id="fill-dim-region",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.region_table_insert.format(**VARIABLE_DICT),
    )

    fill_time_task = PostgresOperator(
        task_id="fill-dim-time",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.time_table_insert.format(**VARIABLE_DICT),
    )

    fill_fact_delta_task = PostgresOperator(
        task_id="fill_fact_delta_trips",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.fact_delta_trips_table_insert.format(**VARIABLE_DICT),
    )

    fill_fact_trips_task = PostgresOperator(
        task_id="fill_fact_trips",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.fact_trips_table_insert.format(**VARIABLE_DICT),
    )

    fill_group_fact_trips_task = PostgresOperator(
        task_id="fill_group_fact_trips",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=DMLScripts.fact_group_trips_table_insert.format(**VARIABLE_DICT),
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        dag=dag,
        conn_id=POSTGRES_CONN_ID,
        tables=[
            f"{PG_FINAL_SCHEMA}.dim_region",
            f"{PG_FINAL_SCHEMA}.dim_event_time",
            f"{PG_FINAL_SCHEMA}.fact_trips",
            f"{PG_FINAL_SCHEMA}.fact_group_trips",
        ],
    )

    (
        start_task
        >> [create_stage_schema_and_tables_task, create_final_schema_and_tables_task]
        >> load_csv_task
        >> [fill_dim_datasource_task, fill_region_task, fill_time_task]
        >> fill_fact_delta_task
        >> fill_fact_trips_task
        >> fill_group_fact_trips_task
        >> run_quality_checks
    )
