from ingest_on_demand.sql_scripts import ddl_scripts
from ingest_on_demand.sql_scripts import dml_scripts

# Schemas
pg_logs_schema = "logs"
pg_stage_schema = "trips_stage"
pg_final_schema = "trips"


# Load data
download_url = "https://drive.google.com/uc?id={FILE_ID}"
csv_table_name = f"{pg_stage_schema}.trips"

# PG configuration
pg_db = "postgres"
pg_host = "127.0.0.1"


# Logs
log_name = "trip_etl"

# user_owner
owner = "postgres"

events = {
    "create_schemas": ddl_scripts.create_schema_queries,
    "create_tables": ddl_scripts.create_table_queries,
    "update_dimension_tables": dml_scripts.update_dimension_tables_queries,
    "update_fact_tables": dml_scripts.update_fact_tables_queries,
}
