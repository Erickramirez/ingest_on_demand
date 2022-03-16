import pandas as pd
import sqlalchemy
import settings
from typing import Optional


def pg_get_data(query: str, params: Optional[dict] = None) -> dict:
    """
        Get data from Postgresql to df
    :param pg_host: ip or name for the PG server instance
    :param username: PG login
    :param password:  PG password
    :param pg_db: db used to run the query
    :param query: PG query select
    :param params: use dictionary to avoid sql injection
    :return: dataframe with the query result
    """

    pg_host = settings.pg_host
    pg_db = settings.pg_db
    password = settings.pg_password
    username = settings.pg_username

    conn_string = f"{username}:{password}@{pg_host}/{pg_db}"
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{conn_string}", executemany_mode="values"
    )
    connection = engine.connect()
    data_df = pd.read_sql(sql=query, con=connection, params=params)
    data = data_df.to_dict("records")
    connection.close()
    engine.dispose()
    return data
