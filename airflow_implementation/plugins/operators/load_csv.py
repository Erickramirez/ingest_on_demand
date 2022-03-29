from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import urllib.request


class LoadCSVFromDriveOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, conn_id, uri_csv_file, table_name, *args, **kwargs):
        """
        Load csv operator initialization
        :param conn_id: connection to specific database (in this case postgreSQL)
        :param uri_csv_file:  uri in this format: https://drive.google.com/file/d/{FILE_ID}
        :param table_name: table name to save the data
        """

        super(LoadCSVFromDriveOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.uri_csv_file = uri_csv_file
        self.table_name = table_name

    def delete_file_if_exists(self, file_path):
        if os.path.isfile(file_path):
            os.remove(file_path)
            self.log.info(f"delete the file  {file_path}")

    def execute(self, context):
        """
        execute operation to validate the quality of the data, specifically if the tables has data (count>0)
        """
        self.log.info(f"LoadCSVOperatorOperator--> Begin")

        posgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"    connected to {self.conn_id}")

        file_path = "csv_file.csv"

        file_id = self.uri_csv_file.split("/")[-2]
        uri = f"https://drive.google.com/uc?id={file_id}"

        self.delete_file_if_exists(file_path)
        urllib.request.urlretrieve(uri, file_path)
        self.log.info(f"download file {file_path}")

        # with open(file_path, "r") as f:
        #    next(f)  # To Skip the header row.
        posgres_hook.run(f"TRUNCATE TABLE {self.table_name}")
        posgres_hook.copy_expert(
            f"COPY {self.table_name} FROM STDIN WITH (FORMAT CSV, header)", file_path
        )

        self.delete_file_if_exists(file_path)

        self.log.info(f"LoadCSVOperatorOperator--> End")
