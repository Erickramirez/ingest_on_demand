# Create Schema

create_logs_schema = """
    CREATE SCHEMA IF NOT EXISTS {PG_LOGS_SCHEMA};
"""

create_stage_schema = """
    CREATE SCHEMA IF NOT EXISTS {PG_STAGE_SCHEMA};
"""

create_final_schema = """
    CREATE SCHEMA IF NOT EXISTS {PG_FINAL_SCHEMA};
"""

# CREATE TABLES
# stage tables
stage_trips_table_create = """
    DROP TABLE IF EXISTS {PG_STAGE_SCHEMA}.trips;
    
    CREATE TABLE {PG_STAGE_SCHEMA}.trips(
        region VARCHAR(50),
        origin_coord VARCHAR(255),
        destination_coord VARCHAR(255),
        datetime  timestamp,
        datasource VARCHAR(50));
"""

fact_delta_trips_table_create = """
    DROP TABLE IF EXISTS {PG_STAGE_SCHEMA}.fact_delta_trips;
    
    CREATE TABLE IF NOT EXISTS {PG_STAGE_SCHEMA}.fact_delta_trips(
        event_timestamp TIMESTAMP,
        event_id bytea,
        region_id INT,
        datasource_id INT,
        origin_coord point,
        destination_coord point,
        PRIMARY KEY(event_timestamp, event_id)
    )
"""

# Dimension tables
dimension_region_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_region(
        region_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        region VARCHAR(50));
"""

dimension_datasource_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_datasource(
        datasource_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        datasource VARCHAR(50));
"""

dimension_time_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_event_time(
        event_timestamp TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT);
"""

# Fact tables
fact_trips_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_trips(
        event_timestamp TIMESTAMP,
        event_id bytea,
        region_id INT,
        datasource_id INT,
        origin_coord point,
        destination_coord point,
        PRIMARY KEY(event_timestamp, event_id)
    ) PARTITION BY RANGE (event_timestamp);
    
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_trips_p2018_05 PARTITION OF {PG_FINAL_SCHEMA}.fact_trips
        FOR VALUES FROM ('2018-05-01 00:00:00') TO ('2018-06-01 00:00:00')
    TABLESPACE pg_default;
"""

fact_group_trips_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_group_trips(
        event_date TIMESTAMP,
        region_id INT,
        box_coord BOX,
        number_of_trips INT,
        PRIMARY KEY(event_date, region_id)
    ) PARTITION BY RANGE (event_date);

    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_group_trips_p2018_05 PARTITION OF {PG_FINAL_SCHEMA}.fact_group_trips
        FOR VALUES FROM ('2018-05-01 00:00:00') TO ('2018-06-01 00:00:00')
    TABLESPACE pg_default;
"""

# logs table
trips_log_table_create = """
    CREATE TABLE IF NOT EXISTS {PG_LOGS_SCHEMA}.trips_log(
        log_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        log_name varchar(50),
        pipeline_version_instance varchar(50),
        event varchar(50),
        event_type varchar(10),
        execution_date_time TIMESTAMP,
        duration_sec FLOAT
    ) 
"""

# Query list
create_schema_queries = [
    create_logs_schema,
    create_stage_schema,
    create_final_schema,
]
create_table_queries = [
    stage_trips_table_create,
    dimension_region_table_create,
    dimension_datasource_table_create,
    dimension_time_table_create,
    fact_delta_trips_table_create,
    fact_trips_table_create,
    fact_group_trips_table_create,
    trips_log_table_create,
]
