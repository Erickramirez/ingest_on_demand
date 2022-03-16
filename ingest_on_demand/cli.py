import click

from ingest_on_demand import __version__
from ingest_on_demand import etl
from ingest_on_demand import settings

VARIABLE_DICT = {
    "PG_LOGS_SCHEMA": settings.pg_logs_schema,
    "PG_STAGE_SCHEMA": settings.pg_stage_schema,
    "PG_FINAL_SCHEMA": settings.pg_final_schema,
    "OWNER": settings.owner,
}


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


# drive2pg
@cli.command()
@click.option(
    "--uri-csv-file",
    type=str,
    envvar="URI_CSV_FILE",
    default="'https://drive.google.com/file/d/14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU/view?usp=sharing'",
    help="uri in this format: https://drive.google.com/file/d/{FILE_ID}",
)
@click.option(
    "--local-file-path",
    type=str,
    envvar="LOCAL_FILE_PATH",
    default="input/trips.csv",
    help="local file path",
)
@click.option(
    "--pipeline-version-instance",
    type=str,
    envvar="PIPELINE_VERSION_INSTANCE",
    default="20220301",
    help="version of pipeline to run",
)
@click.option(
    "--pg-username",
    type=str,
    envvar="PG_USERNAME",
    default="postgres",
    help="PG username",
)
@click.option(
    "--pg-password",
    type=str,
    envvar="PG_PASSWORD",
    default="postgres",
    help="PG password",
)
def load_csv(
    uri_csv_file: str,
    local_file_path: str,
    pipeline_version_instance: str,
    pg_username: str,
    pg_password: str,
) -> None:
    """
        Download and load data from Google Drive to PostgreSQL
    :param uri_csv_file:  uri in this format: https://drive.google.com/file/d/{FILE_ID}
    :param local_file_path:  local file path
    :param pipeline_version_instance:  version of pipeline to run (to group the logs)
    :param pg_username: PG username
    :param pg_password: PG password
    :return: None
    """
    log_name = settings.log_name
    pg_db = settings.pg_db
    pg_host = settings.pg_host
    file_id = uri_csv_file.split("/")[-2]
    uri_csv_file = settings.download_url.format(FILE_ID=file_id)
    table_name = settings.csv_table_name
    etl.load_csv(
        uri=uri_csv_file,
        file_path=local_file_path,
        variable_dict=VARIABLE_DICT,
        pipeline_version_instance=pipeline_version_instance,
        log_name=log_name,
        event="load_csv_file",
        pg_host=pg_host,
        username=pg_username,
        password=pg_password,
        pg_db=pg_db,
        table_name=table_name,
    )


# pg2pg
@cli.command()
@click.option(
    "--event_name",
    type=str,
    envvar="EVENT_NAME",
    required=True,
    help="task name to execute",
)
@click.option(
    "--pipeline-version-instance",
    type=str,
    envvar="PIPELINE_VERSION_INSTANCE",
    default="20220301",
    help="version of pipeline to run",
)
@click.option(
    "--pg_username",
    type=str,
    envvar="PG_USERNAME",
    default="postgres",
    help="PG username",
)
@click.option(
    "--pg_password",
    type=str,
    envvar="PG_PASSWORD",
    default="postgres",
    help="PG password",
)
@click.option(
    "--local-file-path",
    type=str,
    envvar="LOCAL_FILE_PATH",
    default="input/trips.csv",
    help="local file path",
)
def execute_pg_task(
    event_name: str,
    pg_username: str,
    pg_password: str,
    pipeline_version_instance: str,
    local_file_path: str,
) -> None:
    """
        Perform SQL operation in PostgreSQL
    :param event_name: event or task name to execute
    :param pg_username: PG username
    :param pg_password: PG password
    :param pipeline_version_instance:  version of pipeline to run (to group the logs)
    :param local_file_path: local file path
    :return:
    """
    log_name = settings.log_name
    query_list = settings.events[event_name]
    pg_db = settings.pg_db
    pg_host = settings.pg_host

    VARIABLE_DICT["FILE_PATH"] = local_file_path

    etl.execute_pg_tasks(
        pg_host=pg_host,
        username=pg_username,
        password=pg_password,
        pg_db=pg_db,
        query_list=query_list,
        variable_dict=VARIABLE_DICT,
        pipeline_version_instance=pipeline_version_instance,
        log_name=log_name,
        event=event_name,
    )


if __name__ == "__main__":
    cli()
