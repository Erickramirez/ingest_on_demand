import datetime as dt
import logging as log
import os
import sys
import urllib.request

import psycopg2
import sqlalchemy
from tenacity import retry, stop_after_attempt, wait_random

# logging
log_formatting = "%(asctime)s %(levelname)7s %(message)s"
log.basicConfig(format=log_formatting)
log.getLogger().setLevel(log.INFO)
LOG_COLUMNS = [
    "log_name",
    "pipeline_version_instance",
    "event",
    "event_type",
    "execution_date_time",
]
MAX_TASK_RETRIES = 3


def save_log_record(
    event_information: dict,
    pg_host: str,
    username: str,
    password: str,
    pg_db: str,
    query: str,
) -> None:
    """

    :param event_information:
    :param pg_host: host of Postgresql
    :param username: username to log in
    :param password: password to log in
    :param pg_db: database to connect
    :param query: query to execute
    :return: None
    """
    try:
        column_names = ",".join(list(event_information.keys()))
        values = ",".join(f"'{w}'" for w in list(event_information.values()))

        query = query.format(COLUMN_NAMES=column_names, VALUES=values)
        execute_sql_query(
            pg_host=pg_host,
            username=username,
            password=password,
            pg_db=pg_db,
            query=query,
        )
    except Exception as e:
        log.warning(f"Error logging: {e}")


def add_logging():
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            event_information = {}  # Dictionary used to save the data for logs
            for column_name in LOG_COLUMNS:
                event_information[column_name] = kwargs.get(column_name)
            start_time = dt.datetime.now()
            event_information["execution_date_time"] = start_time
            event_information["event_type"] = "start"
            event_information["duration_sec"] = 0
            pg_host = kwargs.get("pg_host")
            username = kwargs.get("username")
            password = kwargs.get("password")
            pg_db = kwargs.get("pg_db")
            variable_dict = kwargs.get("variable_dict")

            query = "INSERT INTO {PG_LOGS_SCHEMA}.trips_log ({{COLUMN_NAMES}}) VALUES({{VALUES}})".format(
                **variable_dict
            )
            save_log_record(
                event_information=event_information,
                pg_host=pg_host,
                username=username,
                password=password,
                pg_db=pg_db,
                query=query,
            )

            try:
                f(*args, **kwargs)

                event_information["event_type"] = "end"
                event_information["execution_date_time"] = dt.datetime.now()
                event_information["duration_sec"] = (
                    dt.datetime.now() - start_time
                ).total_seconds()
                save_log_record(
                    event_information=event_information,
                    pg_host=pg_host,
                    username=username,
                    password=password,
                    pg_db=pg_db,
                    query=query,
                )
            except:
                event_information["event_type"] = "failed"
                event_information["execution_date_time"] = dt.datetime.now()
                event_information["duration_sec"] = (
                    dt.datetime.now() - start_time
                ).total_seconds()
                save_log_record(
                    event_information=event_information,
                    pg_host=pg_host,
                    username=username,
                    password=password,
                    pg_db=pg_db,
                    query=query,
                )
                log.error("Unexpected error:", sys.exc_info()[0])
                raise

        return wrapped_f

    return wrap


def load_csv_data(
    pg_host: str,
    username: str,
    password: str,
    pg_db: str,
    table_name: str,
    file_path: str,
) -> None:
    """
        load local csv file into posgreSQL
    :param pg_host: host of Postgresql
    :param username: username to log in
    :param password: password to log in
    :param pg_db: database to connect
    :param table_name: table name to save the data
    :param file_path: local file path
    :return:
    """
    conn_string = f"host='{pg_host}' dbname='{pg_db}' user='{username}' password='{password}' port="

    with psycopg2.connect(conn_string) as connection:
        cur = connection.cursor()
        with open(file_path, "r") as f:
            next(f)  # To Skip the header row.
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)", f)
        connection.commit()


def execute_sql_query(
    pg_host: str, username: str, password: str, pg_db: str, query: str
) -> None:
    """
        Execute query in postgreSQL
    :param pg_host: host of Postgresql
    :param username: username to log in
    :param password: password to log in
    :param pg_db: database to connect
    :param query: SQL query to execute
    :return: None
    """

    conn_string = f"{username}:{password}@{pg_host}/{pg_db}"
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{conn_string}", executemany_mode="values"
    )
    with engine.connect().execution_options(autocommit=True) as connection:
        connection.execute(query)
        log.info(f"the following command ran successfully: {query}")
        connection.close()


# Tasks
@add_logging()
@retry(stop=stop_after_attempt(MAX_TASK_RETRIES), wait=wait_random(min=1, max=2))
def load_csv(
    uri: str,
    file_path: str,
    pg_host: str,
    username: str,
    password: str,
    pg_db: str,
    table_name: str,
    **kwargs,
) -> None:
    """
        Download and load data from Google Drive to PostgreSQL
    :param uri: uri to download the file
    :param file_path: local file path
    :param pg_host: host of Postgresql
    :param username: username to log in
    :param password: password to log in
    :param pg_db: database to connect
    :param table_name: table name to save the data
    :param kwargs: keyword argument used for logging
    :return: None
    """
    if not (os.path.isfile(file_path)):
        urllib.request.urlretrieve(uri, file_path)
        log.info(f"download file {file_path}")
    else:
        log.info(f"the files already exists {file_path}")

    load_csv_data(
        pg_host=pg_host,
        username=username,
        password=password,
        pg_db=pg_db,
        table_name=table_name,
        file_path=file_path,
    )


@add_logging()
@retry(stop=stop_after_attempt(MAX_TASK_RETRIES), wait=wait_random(min=1, max=2))
def execute_pg_tasks(
    pg_host: str,
    username: str,
    password: str,
    pg_db: str,
    query_list: list,
    variable_dict: dict,
    **kwargs,
) -> None:
    """
        Execute a task with multiple queries
    :param pg_host: host of Postgresql
    :param username: username to log in
    :param password: password to log in
    :param pg_db: database to connect
    :param query_list: list of queries to execute
    :param variable_dict: dictionary to update the queries
    :param kwargs: keyword argument used for logging
    :return:
    """
    for query in query_list:
        query = query.format(**variable_dict)
        execute_sql_query(
            pg_host=pg_host,
            username=username,
            password=password,
            pg_db=pg_db,
            query=query,
        )
